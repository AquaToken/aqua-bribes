from django.conf import settings
from django.utils import timezone

from stellar_sdk import Asset

from aquarius_bribes.bribes.bribe_processor import BribeProcessor
from aquarius_bribes.bribes.exceptions import NoPathForConversionError
from aquarius_bribes.bribes.loader import BribesLoader
from aquarius_bribes.bribes.models import Bribe
from aquarius_bribes.taskapp import app as celery_app


@celery_app.task(ignore_result=True)
def load_bribes():
    loader = BribesLoader(settings.BRIBE_WALLET_ADDRESS, settings.BRIBE_WALLET_SIGNER)
    loader.load_bribes()


@celery_app.task(ignore_result=True)
def claim_bribes():
    ready_to_claim = Bribe.objects.filter(unlock_time__lte=timezone.now())
    
    aqua = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)
    bribe_processor = BribeProcessor(settings.BRIBE_WALLET_ADDRESS, settings.BRIBE_WALLET_SIGNER, aqua)

    for bribe in ready_to_claim:
        try:
            response = bribe_processor.claim_and_convert(bribe)
            bribe.status = Bribe.STATUS_ACTIVE
            bribe.save()
        except NoPathForConversionError:
            bribe.status = Bribe.STATUS_NO_PATH_FOR_CONVERSION
            bribe.save()
        except Exception as e:
            message = bribe.message or ''
            message += '\n' + str(e)
            bribe.message = message
            print (message)
            bribe.status = Bribe.STATUS_FAILED_CLAIM
            bribe.save()


@celery_app.task(ignore_result=True)
def return_bribes():
    ready_to_return = Bribe.objects.filter(status=Bribe.STATUS_NO_PATH_FOR_CONVERSION)
    aqua = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)
    bribe_processor = BribeProcessor(settings.BRIBE_WALLET_ADDRESS, settings.BRIBE_WALLET_SIGNER, aqua)

    for bribe in ready_to_return:
        try:
            response = bribe_processor.claim_and_return(bribe)
            bribe.status = Bribe.STATUS_RETURNED
            bribe.save()
        except Exception as e:
            message = bribe.message or ''
            message += '\n' + str(e)
            bribe.message = message
            print (message)
            bribe.status = Bribe.STATUS_FAILED_RETURN
            bribe.save()
