from typing import Dict, List

from stellar_sdk.soroban_rpc import Transaction

from aquarius_bribes.utils.rpc import get_rpc_server


class LedgerTransactionsCollector:
    def __init__(self, ledger_number: int, page_limit: int = 200) -> None:
        self.ledger_number: int = ledger_number
        self.page_limit: int = page_limit
        self.soroban_server = get_rpc_server()

    def get_transaction(self, transaction_hash: str) -> Transaction:
        return self.soroban_server.get_transaction(transaction_hash)

    def get_transactions_map(self) -> Dict[str, Transaction]:
        transactions = self._load_transactions()

        return {tr.transaction_hash: tr for tr in transactions}

    def _load_transactions(self) -> List[Transaction]:
        ledger_transactions = []
        start_ledger = self.ledger_number
        cursor = None

        while True:
            response = self.soroban_server.get_transactions(
                start_ledger=start_ledger, limit=self.page_limit, cursor=cursor,
            )
            transactions = self._filter_response_transactions(response.transactions)
            ledger_transactions.extend(transactions)

            if len(transactions) < self.page_limit:
                break

            if start_ledger:
                start_ledger = 0

            cursor = response.cursor

        return ledger_transactions

    def _filter_response_transactions(self, transactions: List[Transaction]) -> List[Transaction]:
        return [tr for tr in transactions if tr.ledger == self.ledger_number]
