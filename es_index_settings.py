index_settings = {
    "settings": {
        "analysis": {
            "filter": {
                "arabic_stop": {"type": "stop", "stopwords": "_arabic_"},
                "arabic_stemmer": {"type": "stemmer", "language": "arabic"},
            },
            "analyzer": {
                "default": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "decimal_digit",
                        "arabic_stop",
                        "arabic_normalization",
                        "arabic_stemmer",
                    ],
                }
            },
        }
    }
}
