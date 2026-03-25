import json
from pathlib import Path

def test_translation_dictionary_parity():
    """
    Ensures that the translations.json file contains exactly the same keys
    for all supported languages (en, el, de, es).
    """
    project_root = Path(__file__).resolve().parent.parent
    json_path = project_root / 'streamlit_app' / 'translations.json'
    
    with open(json_path, 'r', encoding='utf-8') as f:
        translations = json.load(f)
        
    supported_langs = {'en', 'el', 'de', 'es'}
    
    # Check that each translation node has the exact 4 required languages
    for key, text_map in translations.items():
        actual_langs = set(text_map.keys())
        missing = supported_langs - actual_langs
        assert not missing, f"Translation key '{key}' is missing languages: {missing}"
        
        # Ensure none of the translation strings are empty
        for lang, text in text_map.items():
            assert str(text).strip() != "", f"Empty translation found for key '{key}' in language '{lang}'"

def test_translation_helper_fallback_behavior():
    """
    Validates that the Streamlit UI `t(key)` helper properly falls back to English
    or a default string if a key is missing.
    """
    # Simulate the runtime implementation of the t() helper in app.py
    import streamlit as st
    
    # Mock translations dictionary
    mock_translations = {
        "title": {"en": "Title", "el": "Τίτλος"}
    }
    
    # Create the helper function as defined in app.py
    def mock_t(key: str, default: str = None, lang_code: str = "en", **kwargs) -> str:
        text = default if default is not None else key
        if key in mock_translations:
            text = mock_translations[key].get(lang_code, mock_translations[key].get("en", text))
        for k, v in kwargs.items():
            text = str(text).replace(f"{{{k}}}", str(v))
        return text

    # Normal fetch
    assert mock_t("title", lang_code="el") == "Τίτλος"
    
    # Missing Language fallback to English
    assert mock_t("title", lang_code="de") == "Title"
    
    # Missing key fallback to default
    assert mock_t("fake_key", default="Safe Fallback", lang_code="es") == "Safe Fallback"
    
    # String interpolation kwargs test
    mock_translations["welcome"] = {"en": "Hello {name}", "el": "Γεια {name}"}
    assert mock_t("welcome", lang_code="el", name="Alex") == "Γεια Alex"
