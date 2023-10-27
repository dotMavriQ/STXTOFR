class STXTOFRError(Exception):
    pass


class ProviderFetchError(STXTOFRError):
    pass


class NormalizationError(STXTOFRError):
    pass


class RecordNotFound(STXTOFRError):
    pass


class ActiveRunError(STXTOFRError):
    pass

