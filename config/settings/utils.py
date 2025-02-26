from stellar_sdk import Asset


def parse_delegatable_asset_config(config: str) -> (Asset, Asset):
    asset, delegation_asset = config.split(';')

    code, issuer = asset.split(':')
    delegation_code, delegation_issuer = delegation_asset.split(':')

    asset = Asset(code, issuer)  # Raise exception if the asset is invalid.
    delegated_asset = Asset(delegation_code, delegation_issuer)  # Raise exception if the asset is invalid.

    return asset, delegated_asset
