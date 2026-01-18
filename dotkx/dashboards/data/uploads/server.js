const fs = require('fs');
const path = require('path');

let port = process.argv[2] || 3000;
let http = process.argv[3] ? require(process.argv[3]) : require("http");
const httpServer = http.createServer(requestHandler);
httpServer.listen(port, () => {console.log('server is listening on port '+ port)});

function requestHandler(req, res){
  if(req.url === '/'){
    sendIndexHtml(res);
  }else if( req.url === '/list'){
    sendListOfUploadedFiles(res);
  }else if( /\/downloads\/[^\/]+$/.test(req.url)){
    sendUploadedFile(req.url, res);
  }else if( /\/upload\/[^\/]+$/.test(req.url) ){
    saveUploadedFile(req, res)
  }else{
    sendIndexHtml(res);
  }
}

function sendIndexHtml(res){
  let indexFile = path.join(__dirname, 'index.html');
  fs.readFile(indexFile, (err, content) => {
    if(err){
      res.writeHead(404, {'Content-Type': 'text'});
      res.write('File Not Found!');
      res.end();
    }else{
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.write(content);
      res.end();
    }
  })
}

function sendListOfUploadedFiles(res){
  let uploadDir = path.join(__dirname, 'downloads');
  let fileDetails = [];
  fs.readdir(uploadDir, (err, files) => {
    if(err){
      console.log(err);
      res.writeHead(400, {'Content-Type': 'application/json'});
      res.write(JSON.stringify(err.message));
      res.end();
    }else{
      res.writeHead(200, {'Content-Type': 'application/json'});
	  
	  files.forEach(file => {
		const stat = fs.statSync(uploadDir + "/" + file);
		fileDetails.push({
			"fileName": file,
			"fileSize": stat.size,
			"fileLastModified": stat.mtime
		});
	  });
      res.write(JSON.stringify(fileDetails));
      res.end();
    }
  })
}


function sendUploadedFile(url, res){
  let file = path.join(__dirname, url);
  fs.readFile(file, (err, content) => {
    if(err){
      res.writeHead(404, {'Content-Type': 'text'});
      res.write('File Not Found!');
      res.end();
    }else{
      res.writeHead(200, {'Content-Type': 'application/octet-stream'});
      res.write(content);
      res.end();
    }
  })
}


function saveUploadedFile(req, res){
  console.log('saving uploaded file');
  let fileName = path.basename(req.url);
  let file = path.join(__dirname, 'downloads', fileName)
  req.pipe(fs.createWriteStream(file));
  req.on('end', () => {
    res.writeHead(200, {'Content-Type': 'text'});
    res.write('uploaded successfully');
    res.end();
  });
  req.on('error', function(err) {
	res.writeHead(200, {'Content-Type': 'text'});
    res.write('upload unsuccessful');
    res.end();;
  });
}

function sendInvalidRequest(res){
  res.writeHead(400, {'Content-Type': 'application/json'});
  res.write('Invalid Request');
  res.end(); 
}