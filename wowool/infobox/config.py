import json


class Config:
    """
    General configuration object.
    """

    def __init__(self):

        self._languages = [
            {"name": "english", "code": "en"},
            {"name": "french", "code": "fr"},
            {"name": "dutch", "code": "nl"},
            {"name": "german", "code": "de"},
            {"name": "portuguese", "code": "po"},
            {"name": "swedish", "code": "sv"},
            {"name": "spanish", "code": "es"},
            {"name": "danish", "code": "dk"},
            {"name": "italian", "code": "it"},
            {"name": "norwegian", "code": "no"},
            {"name": "russian", "code": "ru"},
        ]
        self._language_to_code, self._code_to_language = self._generate_language_maps()

    def _generate_language_maps(self):
        self.l2c = {}
        self.c2l = {}
        for language_info in self._languages:
            self.l2c[language_info["name"]] = language_info["code"]
            self.c2l[language_info["code"]] = language_info["name"]
        return self.l2c, self.c2l

    @property
    def languages(self):
        """ :return: a (``list``) with the available languages """
        return self._languages

    def get_language(self, code: str):
        """
        :param str code: get the language for a give code.
        :return: (``str``) : ex english for the code en
        """
        _code = code.lower()
        retval = self._code_to_language[_code] if _code in self._code_to_language else None
        if not retval:
            return code if code in self._language_to_code else None
        return retval

    def get_language_code(self, language: str):
        """
        :param str code: get the code for a give language.
        :return: (``str``) : ex en for the language english
        """
        _language = language.lower()
        retval = self._language_to_code[_language] if _language in self._language_to_code else None
        if not retval:
            return language if language in self._code_to_language else None
        return retval

    def __str__(self):
        info = {"languages": self.languages}
        return json.dumps(info)


config = Config()
