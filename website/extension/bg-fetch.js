chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({
    id: "translate",
    title: "Tails Argos",
    contexts: ["selection"]
  });
});

chrome.contextMenus.onClicked.addListener((info) => {
  const selectedText = info.selectionText;
  if (!selectedText) return;

  chrome.tabs.create({ url: "about:blank" }, (tab) => {
    chrome.scripting.executeScript({
      target: { tabId: tab.id },
      files: ['inject_post.js'],
       args: [selectedText]
    });
  });
});