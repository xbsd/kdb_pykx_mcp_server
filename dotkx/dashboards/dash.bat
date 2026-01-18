START "Upload Server" node data\uploads\server.js 4200 "http"
START "Demo Data" q sample\demo.q -u 1
q dash.q -p 10001 -u 1
PAUSE
