from abc import ABC, abstractmethod

class TaggerInterface(ABC):
    """
    Defines the contract for any 'tagger' or 'processor' class.
    Any class that implements this interface must have a `process` method.
    """

    @abstractmethod
    def process(self, text: str) -> dict:
        """
        Processes the given text and returns a dictionary of results.

        :param text: The text content of the paper.
        :return: A dictionary where keys are the column names and values are the extracted data.
        """
        pass