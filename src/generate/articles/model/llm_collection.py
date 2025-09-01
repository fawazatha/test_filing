import os
from llama_index.llms.groq import Groq
import dotenv
from langchain.chat_models import init_chat_model

dotenv.load_dotenv()

class LLMCollection:
    """
    @brief Singleton class to manage a collection of LLM (Large Language Model) instances.
    This class ensures that only one instance of the LLMCollection exists and provides methods to add and retrieve LLM instances.
    """
    _instance = None

    def __new__(cls):
        """
        @brief Creates a new instance of LLMCollection if it doesn't already exist.
        @return The singleton instance of LLMCollection.
        """
        if cls._instance is None:
            cls._instance = super(LLMCollection, cls).__new__(cls)
            cls._instance._llms = [
                init_chat_model(
                    "llama3-70b-8192",
                    model_provider="groq",
                    temperature=0.0,
                ),
                init_chat_model(
                    "llama-3.3-70b-versatile",
                    model_provider="groq",
                    temperature=0.0,
                )
            ]
        return cls._instance

    def add_llm(self, llm):
        """
        @brief Adds a new LLM instance to the collection.
        @param llm The LLM instance to be added to the collection.
        """
        self._llms.append(llm)

    def get_llms(self):
        """
        @brief Retrieves the list of LLM instances in the collection.
        @return A list of LLM instances.
        """
        return self._llms

# Example usage:
# llm_collection = LLMCollection()
# llm_collection.add_llm("LLM1")
# llm_collection.add_llm("LLM2")
# print(llm_collection.get_llms())