from django.conf import settings


def google_analytics(request):
    def ga_enabled():
        # if DEBUG
        if settings.DEBUG:
            return False

        # if there are no GA_CODE provided
        if not getattr(settings, 'GOOGLE_ANALYTICS_PROPERTY_ID', False):
            return False

        # allow to disable GA from settings via GA_ENABLED = False
        if hasattr(settings, 'GA_ENABLED'):
            return settings.GA_ENABLED
        return True

    context_ext = {
        'GA_ENABLED': ga_enabled(),
        'GA_CODE': getattr(settings, 'GOOGLE_ANALYTICS_PROPERTY_ID', ''),
    }
    return context_ext
