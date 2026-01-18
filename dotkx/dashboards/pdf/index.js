const puppeteer = require("puppeteer");
const { URL } = require("url");
const fs = require('fs');
const config = require('./config');

const PDF_TIMEOUT = config.pdfGenerationTimeout;
const READY_CHECK_INTERVAL = config.checkInterval;
const LOG_TO_FILE = config.logToFile;

(async () => {
    const PROCESS_START = new Date();
    // newline in logs
    const NL = "\n";

    let browser;
    let tickLog = "";

    const delay = (time) => new Promise(r => setTimeout(r, time));

    try {
        browser = await puppeteer.launch({
            args: ["--no-sandbox", "--disable-setuid-sandbox", '--disable-renderer-backgrounding', '--headless', '--disable-gpu', '--disable-dev-shm-usage'],
            // set executablePath in puppeteer.config.js, not here!
            ignoreHTTPSErrors: true,
            // to remove warning, headless mode is set to "old" instead of: true
            // !! not sure why, but to print 1 page using 
            //   "old" took 2.33 seconds, compared to:
            //   "new" took 4.41 seconds
            // more details: https://developer.chrome.com/articles/new-headless/
            headless: "old"
        });
        
        const page = await browser.newPage();

        const dashUrl = process.argv[2]; // here we have all parameters
        const filePath = process.argv[3];
        const theme = process.argv[4];
        const token = process.argv[5];

        const url = new URL(dashUrl);

        const documentWidth = parseInt(url.searchParams.get("documentWidth"), 10);
        const documentHeight = parseInt(url.searchParams.get("documentHeight"), 10);
        const standardWidth = url.searchParams.get("width");
        const clientURL = url.searchParams.get("clientURL");
        const useClientUrl = url.searchParams.get("useClientUrl") === "1" ? true : false;
        const divideinpages = false;
        const printFilters = url.searchParams.get("printfilters") === "1" ? true : false;
        const createScreenshot = true;

        const width = standardWidth === "standard" ? 1920 : documentWidth;
        const height = standardWidth === "standard" ? 1080 : documentHeight;
        const hasToken = token !== undefined;
        const hasUserPass = hasToken && token.indexOf(":") !== -1;
        if (hasToken && !hasUserPass) {
            await page.setCookie({
                name: "deltaToken",
                value: token,
                domain: url.hostname,
            });
        }

        const timezone = url.searchParams.get("timezone");
        if (timezone) {
            await page.setCookie({
                name: "dashboard-timezone",
                value: timezone,
                domain: url.hostname,
            });
        }

        //this only affects the theme when the dashboard style property "Theme Switchable" is true
        //see onDocumentChanged() for details
        await page.setCookie({
            name: "dashboard-viewer-theme",
            value: theme || "kx-light",
            domain: url.hostname,
        });

        // best resolution
        page.setViewport({
            width: width,
            height: height,
        });

        // screen media (css)
        await page.emulateMediaType("screen");

        // go to page
        const theUrl = useClientUrl
            ? clientURL + url.hash + url.search
            : url.origin + url.pathname + url.hash + url.search;

        await page.goto(
            // viewstate has to be after hash in order for it to work in appRouter
            theUrl,
            { timeout: PDF_TIMEOUT }
        );

        // login
        if (hasUserPass) {
            await page.evaluate(p => {
                document.querySelector(".username").value = p[0];
                document.querySelector(".password").value = p[1];
                document.querySelector(".login-btn").click();
            }, token.split(":"));
        }

        if (printFilters) {
            // append viewstate to body
            await page.evaluate(text => {
                const filtersDiv = document.createElement("div");
                filtersDiv.className = "pdf-filters-info";
                filtersDiv.innerText = text;
                document.body.appendChild(filtersDiv);
            }, url.searchParams.get("viewstate"));
        }

        do {
            console.log("checking printingStatus");
            const isReadyForPrinting = await page.evaluate(() => { 
                const isReadyFn = window["KXisReadyForPrinting"];
                return isReadyFn ? isReadyFn() : false;
            });

            if (isReadyForPrinting || (new Date() - PROCESS_START > PDF_TIMEOUT)) {
                break;
            }

            tickLog += ".";
            
            await delay(READY_CHECK_INTERVAL);
        } while (true)

        // create pdf or capture screenshot:
        if (createScreenshot) {
            await page.screenshot({
                path: filePath.split(".pdf")[0] + ".png",
                fullPage: true,
                printBackground: true,
            });
        } else {
            // if over the time limit, give pdf function at least 1 second
            const remainingTimeout = Math.max(1000, PDF_TIMEOUT - (new Date() - PROCESS_START));

            if (divideinpages) {
                await page.pdf({
                    timeout: remainingTimeout,
                    path: filePath,
                    format: "A4",
                    //   width: width,
                    //   height: height,
                    //   fullPage: true,
                    printBackground: true,
                });
            } else {
                await page.pdf({
                    timeout: remainingTimeout,
                    path: filePath,
                    width: width, // 1620,
                    height: height, // 1000,
                    printBackground: true,
                });
            }
        }
    } catch (ex) {
        console.log(ex);
        
        const dt = (new Date()).toISOString() + NL;
        if (LOG_TO_FILE) {
            fs.appendFileSync("pdf_exceptions.txt", 
                // log environment details
                dt + JSON.stringify(process.env, null, 4) + NL + NL +
                // log process arguments and error details
                dt + JSON.stringify(process.argv, null, 4) + NL + ex + NL + NL);
        }
    } finally {
        if (browser) {
            await browser.close();
        }
        
        const PROCESS_END = new Date();

        console.log(JSON.stringify(process.argv, null, 4) + NL + ((PROCESS_END - PROCESS_START)/1000).toFixed(2) + " s" + NL + NL);
        if (LOG_TO_FILE) {
            fs.appendFileSync("pdf_stats.txt", 
                (new Date().toISOString()) + NL +
                "CWD: " + process.cwd() + NL + 
                "__dirname: " + __dirname + NL + 
                // log process arguments
                JSON.stringify(process.argv, null, 4) + NL + 
                ((PROCESS_END - PROCESS_START)/1000).toFixed(2) + "s " + tickLog + NL + NL);
        }
    }
    // done
})();