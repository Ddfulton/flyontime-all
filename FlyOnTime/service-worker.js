// This is the service worker script, which executes in its own context
// when the extension is installed or refreshed (or when you access its console).
// It would correspond to the background script in chrome extensions v2.

console.log("This prints to the console of the service worker (background script)")

// Importing and using functionality from external files is also possible.
importScripts('service-worker-utils.js')

chrome.runtime.onMessage.addListener(function(request, sender, sendResponse) {

  // getCounter
  if (request.action === "getCounter") {

    // https://app.gumroad.com/api#licenses
    // They get license key
    // One-time entry of license key
    // POST it over https to gumroad
    // If valid, authorized_user = true forever


    // Get the counter from chrome.storage.local
    chrome.storage.local.get("counter", function(result) {

      // If it exists, check it and deal with it
      if (result.hasOwnProperty("counter")) {
        chrome.storage.local.set({ counter: result.counter + 1 }, function() {

          // If they've over free limit
          if (result.counter > 100) {
            // Redirect them to onboarding tab
            // chrome.tabs.create({
            //   url: "onboarding.html",
            // });
          }
          sendResponse({counter: result.counter})
          return true;
        });
      } else {  // If it does not exist
        // ...then set it to 1
        chrome.storage.local.set({ counter: 1 }, function() {
          sendResponse({counter: 1})
          return true;
        });
      }
    });

    // sendResponse({ counter: 42 });
    return true; // Indicates that the response will be sent asynchronously
  }
});



// chrome.runtime.onInstalled.addListener(function(details){
//   if(details.reason == "install"){
//       console.log("This is a first install!");
//   }else if(details.reason == "update"){
//       var thisVersion = chrome.runtime.getManifest().version;
//       console.log("Updated from " + details.previousVersion + " to " + thisVersion + "!");
//       chrome.tabs.create({url: 'https://flyontime.app/'});
//   }
// });
