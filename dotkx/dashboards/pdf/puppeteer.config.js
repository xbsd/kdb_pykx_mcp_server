/**
 * @type {import("puppeteer").Configuration}
 */
module.exports = {
    // https://pptr.dev/api/puppeteer.configuration

    // Changes the cache location for Puppeteer, after "npm install", 
    //   "chrome" folder will be created in current folder instead of default location:
    //     Win: "C:\Users\home\.cache\puppeteer\chrome"
    //     Unix: "/app/home/.cache/puppeteer"
    cacheDirectory: __dirname,
    skipDownload: false
  };