"""OCR engine interface. Stub — implement in a later phase."""
from abc import ABC, abstractmethod


class OcrEngine(ABC):
    @abstractmethod
    def extract(self, image_bytes: bytes) -> str:
        # TODO: run OCR on the image and return extracted text.
        raise NotImplementedError
