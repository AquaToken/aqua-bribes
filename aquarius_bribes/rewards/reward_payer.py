from billiard.exceptions import SoftTimeLimitExceeded
from datetime import timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import List

from django.conf import settings
from django.db import models
from django.utils import timezone

from stellar_sdk import Asset
from stellar_sdk.exceptions import BaseHorizonError, NotFoundError
from stellar_sdk.server import Server
from stellar_sdk.transaction_builder import TransactionBuilder, TransactionEnvelope

from aquarius_bribes.bribes.utils import get_horizon
from aquarius_bribes.rewards.models import Payout


class BaseRewardPayer(object):
    payout_class = None

    def __init__(self, bribe, payer_wallet, reward_asset, reward_amount, stop_at=None):
        self.bribe = bribe
        self.asset = reward_asset
        self.payer_wallet = payer_wallet
        self.server = get_horizon()
        self.time_before_check_timeouted_transactions = 5
        self.stop_at = stop_at
        self.reward_amount = reward_amount

    def _clean_rewards(self, rewards):
        raise NotImplementedError()

    def _get_reward_page(self, rewards):
        qs = self._clean_rewards(rewards)
        return list(qs[:100])

    def _get_memo(self):
        raise NotImplementedError()

    def _get_builder(self):
        server_account = self.server.load_account(self.payer_wallet.public_key)
        base_fee = settings.BASE_FEE

        memo = self._get_memo()

        builder = TransactionBuilder(
            source_account=server_account,
            network_passphrase=settings.STELLAR_PASSPHRASE,
            base_fee=base_fee,
        ).add_text_memo(memo)
        return builder

    def _get_payout_instance(self, reward, total_votes):
        raise NotImplementedError()

    def _generate_payouts(self, rewards_page: List, total_votes):
        payouts = []
        for reward in rewards_page:
            payouts.append(
                self._get_payout_instance(reward, total_votes),
            )
        return payouts

    def _append_payment_op(self, builder, payout):
        raise NotImplementedError()

    def _build_transaction(self, payouts) -> TransactionEnvelope:
        builder = self._get_builder()

        for payout in payouts:
            self._append_payment_op(builder, payout)

        return builder.build()

    def _process_page(self, rewards_page: List, total_votes):
        payouts = self._generate_payouts(rewards_page, total_votes)
        transaction_envelope = None

        try:
            # workaround to avoid fails because of unstable horizon
            transaction_envelope = self._build_transaction(payouts)
        except Exception:
            return

        transaction_envelope.sign(self.payer_wallet.secret)

        try:
            response = self.server.submit_transaction(transaction_envelope)

            if response.get('successful', True):
                for payout in payouts:
                    payout.stellar_transaction_id = response['hash']
                self.payout_class.objects.bulk_create(payouts)
        except SoftTimeLimitExceeded as timeout_exc:
            for payout in payouts:
                payout.stellar_transaction_id = transaction_envelope.hash_hex()
                payout.status = self.payout_class.STATUS_FAILED
                payout.message = 'timeout'
            self.payout_class.objects.bulk_create(payouts)
        except BaseHorizonError as submit_exc:
            if getattr(submit_exc, 'status', None) in [504, 522]:
                for payout in payouts:
                    payout.stellar_transaction_id = transaction_envelope.hash_hex()
                    payout.status = self.payout_class.STATUS_FAILED
                    payout.message = 'timeout'
                self.payout_class.objects.bulk_create(payouts)
            else:
                failed_payouts = []

                operation_fail_reasons = submit_exc.extras.get('result_codes', {}).get('operations', [])
                if not operation_fail_reasons:
                    operation_fail_reasons = submit_exc.extras.get('result_codes', {}).get('transaction', 'no_reason')
                    operation_fail_reasons = [operation_fail_reasons] * len(payouts)

                for index, code in enumerate(operation_fail_reasons):
                    if code != 'op_success':
                        payouts[index].message = code
                        payouts[index].status = self.payout_class.STATUS_FAILED
                        failed_payouts.append(payouts[index])

                if operation_fail_reasons:
                    self.payout_class.objects.bulk_create(failed_payouts)
        except Exception as unknown_exc:
            for payout in payouts:
                payout.stellar_transaction_id = transaction_envelope.hash_hex()
                payout.status = self.payout_class.STATUS_FAILED
                payout.message = str(unknown_exc)
            self.payout_class.objects.bulk_create(payouts)

    def _clean_failed_payouts(self, rewards):
        timeouted_transactions = rewards.filter(
            payout__stellar_transaction_id__isnull=False
        ).filter(
            payout__message='timeout'
        ).exclude(
            payout__created_at__gt=timezone.now() - timedelta(minutes=self.time_before_check_timeouted_transactions)
        ).values_list(
            'payout__stellar_transaction_id', flat=True
        ).distinct()

        for tx_hash in timeouted_transactions:
            try:
                tx_data = self.server.transactions().transaction(tx_hash).call()

                if tx_data.get('successful', False) == True:
                    self.payout_class.objects.filter(
                        stellar_transaction_id=tx_hash,
                    ).update(status=self.payout_class.STATUS_SUCCESS)
                else:
                    self.payout_class.objects.filter(stellar_transaction_id=tx_hash).delete()
            except NotFoundError:
                self.payout_class.objects.filter(stellar_transaction_id=tx_hash).delete()

    def _exclude_small_votes(self, votes, total_votes):
        min_votes_value = Decimal(Decimal("0.0000001") * total_votes / self.reward_amount).quantize(
            Decimal('0.0000001'), rounding=ROUND_UP,
        )
        return votes.filter(votes_value__gte=min_votes_value)

    def pay_reward(self, votes):
        self._clean_failed_payouts(votes)

        total_votes = votes.aggregate(total_votes=models.Sum('votes_value'))['total_votes']

        votes = self._exclude_small_votes(votes, total_votes)

        page = self._get_reward_page(votes)
        while page:
            if self.stop_at and timezone.now() > self.stop_at:
                return

            self._process_page(page, total_votes)
            page = self._get_reward_page(votes)


class RewardPayer(BaseRewardPayer):
    payout_class = Payout

    def _clean_rewards(self, rewards):
        qs = rewards
        
        failed_by_unkown_reason = self.payout_class.objects.filter(
            bribe=self.bribe, vote_snapshot__in=qs,
        ).exclude(message__in=[
            'tx_bad_auth', 'tx_bad_seq', 'tx_insufficient_balance', 'tx_insufficient_fee',
        ], status=self.payout_class.STATUS_FAILED).values_list('vote_snapshot_id').distinct()
        qs = qs.exclude(id__in=failed_by_unkown_reason)

        already_payed = rewards.filter(
            payout__status=self.payout_class.STATUS_SUCCESS, payout__bribe=self.bribe,
        ).values_list('id', flat=True)
        qs = qs.exclude(id__in=already_payed)

        return qs

    def _get_memo(self):
        return 'Bribe: {}...{}'.format(self.bribe.market_key_id[:4], self.bribe.market_key_id[-4:])

    def _get_payout_instance(self, vote, total_votes):
        reward_amount = (self.reward_amount * vote.votes_value / total_votes)
        reward_amount = reward_amount.quantize(Decimal('0.0000000'), rounding=ROUND_DOWN)
        return self.payout_class(
            vote_snapshot=vote,
            bribe=self.bribe,
            asset_code=self.asset.code,
            asset_issuer=self.asset.issuer or '',
            reward_amount=reward_amount,
            status=self.payout_class.STATUS_SUCCESS,
        )

    def _append_payment_op(self, builder, payout):
        if self.bribe.asset.type == Asset.native().type:
            builder.append_payment_op(
                destination=payout.vote_snapshot.voting_account,
                source=self.payer_wallet.public_key,
                amount=payout.reward_amount,
            )
        else:
            builder.append_payment_op(
                destination=payout.vote_snapshot.voting_account,
                asset_code=payout.asset_code,
                asset_issuer=payout.asset_issuer,
                source=self.payer_wallet.public_key,
                amount=payout.reward_amount,
            )
