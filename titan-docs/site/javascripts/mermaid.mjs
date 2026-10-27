import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";

mermaid.initialize({
  startOnLoad: false,
  securityLevel: "strict",
});

const renderDiagrams = () => {
  document.querySelectorAll("pre.titan-mermaid-source").forEach((fence) => {
    const diagram = document.createElement("div");
    diagram.className = "mermaid";
    diagram.textContent = fence.textContent;
    fence.replaceWith(diagram);
  });

  const nodes = document.querySelectorAll("div.mermaid:not([data-processed='true'])");
  if (nodes.length) {
    mermaid.run({ nodes });
  }
};

// Re-render after initial load and Material's instant-navigation updates.
if (window.document$) {
  window.document$.subscribe(renderDiagrams);
} else {
  window.addEventListener("DOMContentLoaded", renderDiagrams);
}
