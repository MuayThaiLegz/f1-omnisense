"""Retriever for document RAG with category filtering and MMR.

Provides semantic search over ingested documents. Backend-agnostic:
works with any vectorstore that implements similarity_search,
max_marginal_relevance_search, and similarity_search_with_relevance_scores.

Usage:
    from pipeline.retriever import DocumentRetriever

    retriever = DocumentRetriever(vectorstore=my_vectorstore)
    results = retriever.search("F1 technical regulation 2024", k=5)
    context = retriever.get_relevant_context("tire compound specifications")
"""

from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable


@runtime_checkable
class VectorStoreProtocol(Protocol):
    """Minimal interface a vectorstore must implement."""

    def similarity_search(self, query: str, k: int = 5, filter: dict | None = None) -> list: ...
    def max_marginal_relevance_search(self, query: str, k: int = 5, fetch_k: int = 20, filter: dict | None = None) -> list: ...
    def similarity_search_with_relevance_scores(self, query: str, k: int = 5, filter: dict | None = None) -> list[tuple]: ...


class DocumentRetriever:
    """Retriever for documents with category filtering.

    Wraps any vectorstore implementing VectorStoreProtocol.
    """

    def __init__(self, vectorstore: VectorStoreProtocol):
        self._vectorstore = vectorstore

    @property
    def vectorstore(self) -> VectorStoreProtocol:
        return self._vectorstore

    def search(
        self,
        query: str,
        k: int = 5,
        category: str | None = None,
        search_type: Literal["similarity", "mmr"] = "similarity",
        fetch_k: int = 20,
    ) -> list:
        """Search for relevant documents.

        Parameters
        ----------
        query : search query
        k : number of results
        category : filter by category
        search_type : "similarity" for basic, "mmr" for diverse results
        fetch_k : candidates to fetch before MMR filtering

        Returns
        -------
        List of Document objects with content and metadata
        """
        filter_dict = None
        if category:
            filter_dict = {"category": category}

        if search_type == "mmr":
            return self.vectorstore.max_marginal_relevance_search(
                query,
                k=k,
                fetch_k=fetch_k,
                filter=filter_dict,
            )
        else:
            return self.vectorstore.similarity_search(
                query,
                k=k,
                filter=filter_dict,
            )

    def search_with_scores(
        self,
        query: str,
        k: int = 5,
        category: str | None = None,
    ) -> list[tuple]:
        """Search with relevance scores (0-1, higher = more relevant)."""
        filter_dict = {"category": category} if category else None

        return self.vectorstore.similarity_search_with_relevance_scores(
            query,
            k=k,
            filter=filter_dict,
        )

    def get_relevant_context(
        self,
        query: str,
        k: int = 5,
        min_score: float = 0.2,
    ) -> str:
        """Get formatted context string for LLM prompts."""
        results = self.search_with_scores(query, k=k)

        context_parts = []
        for doc, score in results:
            if score < min_score:
                continue

            source = doc.metadata.get("source_file", "unknown")
            category = doc.metadata.get("category", "unknown")

            context_parts.append(
                f"[Source: {source} | Category: {category} | Relevance: {score:.2f}]\n"
                f"{doc.page_content}\n"
            )

        return "\n---\n".join(context_parts) if context_parts else ""
