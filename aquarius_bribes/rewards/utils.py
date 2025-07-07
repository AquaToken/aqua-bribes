from stellar_sdk import ClaimPredicate


class SecuredWallet(object):
    def __init__(self, public_key, secret):
        self.public_key = public_key
        self.secret = secret


def _get_not_unconditinal_predicate():
    return ClaimPredicate.predicate_not(
        ClaimPredicate.predicate_unconditional(),
    ).to_xdr_object().to_xdr()
