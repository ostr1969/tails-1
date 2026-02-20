chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({
    id: "translate",
    title: "Tails Argos",
    contexts: ["selection"]
  });
});

chrome.contextMenus.onClicked.addListener((info) => {
  const url = "http://132.72.112.48:5001/argos?text=" + encodeURIComponent(info.selectionText);
  chrome.tabs.create({ url });
});

