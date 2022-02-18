class SecuredWallet(object):
    def __init__(self, public_key, secret):
        self.public_key = public_key
        self.secret = secret
