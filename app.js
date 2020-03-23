const path = require('path');
const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const xml2js = require('xml2js');
const session = require('express-session');
const cookieParser = require('cookie-parser');
const MemoryStore = require('memorystore')(session);
const helmet = require('helmet');
const log4js = require('Log4js');
const passport     = require('passport')
const LdapStrategy = require('passport-ldapauth');
const fs = require('fs');

log4js.configure('./config/log4js.json');
const logger = log4js.getLogger('app');
const config = require('./config/config');
const nQSessionService = config.obiee.host+'/analytics-ws/saw.dll?SOAPImpl=nQSessionService';
const xmlViewService = config.obiee.host+'/analytics-ws/saw.dll?SOAPImpl=xmlViewService';
const nQWebCatalogService = config.obiee.host+'/analytics-ws/saw.dll?SoapImpl=webCatalogService';
const wsdl = config.obiee.wsdl;
const httpProxy = config.proxy.url;
const app = express();
const port = process.env.PORT || config.app.port;
const OPTS = {
  server: {
    url: config.ldap.url,
    bindDN: config.ldap.bindDN,
    bindCredentials: config.ldap.bindCredentials,
    searchBase: config.ldap.searchBase,
    searchFilter: config.ldap.searchFilter
  }
};

passport.use(new LdapStrategy(OPTS));
app.use(log4js.connectLogger(log4js.getLogger('http'), { level: 'auto' }));
app.use(helmet());
app.use(express.static('public'));
app.use(cookieParser());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
/*
app.use(session({
  secret: 'Padje02kdps&*$',
  resave: false,
  saveUninitialized: true,
  store: new MemoryStore({
    checkPeriod: 86400000, // prune expired entries every 24h
  }),
  cookie: { maxAge: 86400000 }
}));
*/
app.use(passport.initialize());
app.use(passport.session());

// serialize user object
passport.serializeUser(function (user, done) {
  done(null, user);
});

// deserialize user object
passport.deserializeUser(function (user, done) {
  done(null, user);
});

app.listen(port, () => {
  logger.info(`Listening to requests on http://localhost:${port}`);
});

function checkAuthentication(req,res,next) {
  if(req.isAuthenticated()){
      //req.isAuthenticated() will return true if user is logged in
      next();
  } else{
      res.redirect("/signin");
  }
}

app.get('/signin', (req, res) => {
  res.sendFile(__dirname + '/public/html/index.html');
});

app.post('/OBIlogon', function (req, res) {
  OBIEELogin(config.obiee.username, config.obiee.password, function (sess){
    res.send(sess);
  });

});

app.post('/getSchema', function (req, res) {
    defineSchema(req.body.reportPath, req.body.sessionid, function (schema) {
      res.json(schema);
    });
});


app.post('/login', passport.authenticate('ldapauth', { successRedirect: '/signin',failureRedirect: '/failure'}));



/*
app.get('/extract', checkAuthentication, function (req, res) {
  res.sendFile(__dirname +'/public/html/extract.html');
});


app.get('/extract', function (req, res) {
  res.sendFile(__dirname +'/public/html/extract.html');
});
*/

app.post('/logoff', function (req, res) {

  const soapMessage = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v12="urn://oracle.bi.webservices/v12"> <soapenv:Header/> <soapenv:Body> <v12:logoff> <v12:sessionID>' + req.body.sessionid + '</v12:sessionID> </v12:logoff> </soapenv:Body> </soapenv:Envelope>';
  const requestBody = {
    method: 'POST',
    proxy: httpProxy,
    uri: nQSessionService,
    headers: {
      'content-type': 'text/xml; charset="utf-8"',
      'User-Agent': 'OBIConnector',
    },
    body: soapMessage,
  };

  logger.debug('Logoff in progress = ' + req.body.sessionid);

  request(requestBody, function (error, response, body) {
    if (error) {
      callback(error); return;
    }

    if (response.statusCode != 200) {
      callback(response.statusMessage); return;
    }
    
    res.json(response.statusMessage);

  });

});

function defineSchema(reportPath, sessionid, callback) {

  const requestBody = {
    method: 'POST',
    proxy: httpProxy,
    uri: xmlViewService,
    headers: {
      'content-type': 'text/xml; charset="utf-8"',
      'User-Agent': 'OBIConnector',
    },
    body: '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v12="urn://oracle.bi.webservices/v12"> <soapenv:Header/> <soapenv:Body> <v12:executeXMLQuery> <v12:report> <v12:reportPath>' + reportPath + '</v12:reportPath></v12:report> <v12:outputFormat>SAWRowsetSchema</v12:outputFormat> <v12:executionOptions> <v12:async>False</v12:async> <v12:maxRowsPerPage>65000</v12:maxRowsPerPage> <v12:refresh>False</v12:refresh> </v12:executionOptions> <v12:sessionID>' + sessionid + '</v12:sessionID> </v12:executeXMLQuery> </soapenv:Body> </soapenv:Envelope>',
  };

  request(requestBody, function (error, response, body) {
    if (error) {
      logger.error(error); return;
    }

    if (response.statusCode != 200) {
      logger.error(response.statusMessage); return;
    }

    body = sanitiseXML(body);

    const options = {
      normalizeTags: true,
      ignoreAttrs: true,
      explicitArray: false,
      trim: true,
      tagNameProcessors: [xml2js.processors.stripPrefix],
    };

    xml2js.parseString(body, options, function (err, result) {
      if (err) {
        logger.error(err); return;
      }

      if ('fault' in result.envelope.body) {
        //logger.error(result.body.fault.faultstring);
        return;
      } else {
        let innerXML = result.envelope.body.executexmlqueryresult.return.rowset;
        let queryID = result.envelope.body.executexmlqueryresult.return.queryID;
        let finished = result.envelope.body.executexmlqueryresult.return.finished;

        const options = {
          normalizeTags: true,
          ignoreAttrs: false,
          explicitArray: false,
          tagNameProcessors: [xml2js.processors.stripPrefix],
        };

        // Get Schema and Table definitions
        xml2js.parseString(innerXML, options, function (err, result) {
          if (err) {
            logger.error(err); return;
          }

          let schemaXML = result.rowset.schema.complextype.sequence.element;

          var str=[];

          for (let i in schemaXML) {

            if (schemaXML[i].$['saw-sql:type'] == 'varchar') {
              dt = 'tableau.dataTypeEnum.string';
            } else if (schemaXML[i].$['saw-sql:type'] == 'double') {
              dt = 'tableau.dataTypeEnum.float';
            } else if (schemaXML[i].$['saw-sql:type'] == 'numeric') {
              dt = 'tableau.dataTypeEnum.float';
            } else if (schemaXML[i].$['saw-sql:type'] == 'integer') {
              dt = 'tableau.dataTypeEnum.int';
            } else if (schemaXML[i].$['saw-sql:type'] == 'date') {
              dt = 'tableau.dataTypeEnum.date';
            } else if (schemaXML[i].$['saw-sql:type'] == 'timestamp') {
              dt = 'tableau.dataTypeEnum.datetime';
            } else {
              dt = 'tableau.dataTypeEnum.string';
            }

            str.push({
                "id":(schemaXML[i].$['saw-sql:columnHeading']).replace(/[-,!*&^%_\t\s]/g,''),
                "dataType":dt
            });
          }

          logger.debug('Schema size = ' + str.length);
          return callback(str);
        });
      }
    });
  });
}

function OBIEELogin(username, password, callback) {
  let soapMessage = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:' + wsdl + '="urn://oracle.bi.webservices/' + wsdl + '" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Header/>';
  soapMessage += '<soapenv:Body><' + wsdl + ':logon><' + wsdl + ':name>' + username + '</' + wsdl + ':name><' + wsdl + ':password>' + password + '</' + wsdl + ':password></' + wsdl + ':logon>';
  soapMessage += '</soapenv:Body></soapenv:Envelope>';

  const requestBody = {
    method: 'POST',
    proxy: httpProxy,
    uri: nQSessionService,
    headers: {
      'content-type': 'text/xml; charset="utf-8"',
      'User-Agent': 'OBIConnector',
    },
    body: soapMessage,
  };

  request(requestBody, function (error, response, body) {
    if (error) {
      logger.error(error);
      callback(error); return;
    }

    if (response.statusCode != 200) {
      logger.error(response.statusMessage);
      callback(response.statusMessage); return;
    }

    var body = sanitiseXML(body);

    const options = {
      normalizeTags: true,
      ignoreAttrs: true,
      explicitArray: false,
      tagNameProcessors: [xml2js.processors.stripPrefix],
    };

    xml2js.parseString(body, options, function (err, result) {
      if (err) {
        callback(err);
        return;
      }

      result = result.envelope;
      if ('fault' in result.body) {
        callback(result.body.fault.faultstring);
		    return;
      } else {
        logger.debug(result.body.logonresult.sessionid);
        return callback(result.body.logonresult.sessionid);
      }
    });

  });
}


app.post('/getData', function (req, res) {

  const requestBody = {
    method: 'POST',
    proxy: httpProxy,
    uri: xmlViewService,
    headers: {
      'content-type': 'text/xml; charset="utf-8"',
      'User-Agent': 'OBIConnector',
    },
    body: '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v12="urn://oracle.bi.webservices/v12"> <soapenv:Header/> <soapenv:Body> <v12:executeXMLQuery> <v12:report> <v12:reportPath>' + req.body.reportPath + '</v12:reportPath></v12:report> <v12:outputFormat>SAWRowsetData</v12:outputFormat> <v12:executionOptions> <v12:async>False</v12:async> <v12:maxRowsPerPage>500000</v12:maxRowsPerPage> <v12:refresh>True</v12:refresh> </v12:executionOptions> <v12:sessionID>' + req.body.sessionid + '</v12:sessionID> </v12:executeXMLQuery> </soapenv:Body> </soapenv:Envelope>',
  };

  request(requestBody, function (error, response, body) {
    if (error) {
      logger.error(error); return;
    }

    if (response.statusCode != 200) {
      logger.error(response.statusMessage); return;
    }

    body = sanitiseXML(body);

    const options = {
      normalizeTags: true,
      ignoreAttrs: true,
      explicitArray: false,
      trim: true,
      tagNameProcessors: [xml2js.processors.stripPrefix],
    };

    xml2js.parseString(body, options, function (err, result) {
      if (err) {
        logger.error(err); return;
       }

      if ('fault' in result.envelope.body) {
        logger.error(result.envelope.body);
        return;
      } else {
        let innerXML = result.envelope.body.executexmlqueryresult.return.rowset;
        let queryID = result.envelope.body.executexmlqueryresult.return.queryid;
        let finished = result.envelope.body.executexmlqueryresult.return.finished;

        getNext(queryID, req.body.sessionid, function (error, output, status) {
          logger.debug('Query ID = '+queryID+' 1.1 Finish getting more data = ' + status);
          innerXML+=output;
          while(!status) {
            getNext(queryID, req.body.sessionid, function (error, output, status) {
              logger.debug('Query ID = '+queryID+' 1.2 Finish getting more data = ' + status);
              innerXML+=output;
            });
          }

          const options = {
            normalizeTags: true,
            ignoreAttrs: false,
            explicitArray: false,
            tagNameProcessors: [xml2js.processors.stripPrefix],
          };

          xml2js.parseString(innerXML, options, function (err, result) {
            if (err) {
              logger.error(err); return;
            }

            if (typeof result.rowset.row !== 'undefined') {
              logger.debug('Result Set size = '+ Object.keys(result.rowset.row).length);
              res.json(result.rowset.row);
            } else {
              logger.error('Error = ' + innerXML);
              res.json(innerXML);
            }
          });

        });

      }
    });
  });
});


function getNext(queryid, sessionid, callback) {
  const requestBody = {
    method: 'POST',
    proxy: httpProxy,
    uri: xmlViewService,
    headers: {
      'content-type': 'text/xml; charset="utf-8"',
      'User-Agent': 'OBIConnector',
    },
    body: '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v12="urn://oracle.bi.webservices/v12"> <soapenv:Header/> <soapenv:Body> <v12:fetchNext> <v12:queryID>'+queryid+'</v12:queryID> <v12:sessionID>'+sessionid+'</v12:sessionID> </v12:fetchNext> </soapenv:Body> </soapenv:Envelope>',
  };

  request(requestBody, function (error, response, body) {
    if (error) {
      logger.error(error); return;
    }

    if (response.statusCode != 200) {
      logger.error(response.statusMessage); return;
    }

    body = sanitiseXML(body);

    const options = {
      normalizeTags: true,
      ignoreAttrs: true,
      explicitArray: false,
      trim: true,
      tagNameProcessors: [xml2js.processors.stripPrefix],
    };

    xml2js.parseString(body, options, function (err, result) {
      if (err) {
        logger.error(err); return;
      }

      if ('fault' in result.envelope.body) {
        //logger.error(result.body.fault.faultstring);
        //callback(result.body.fault.faultstring,null,null);
        return;
      } else {
        //logger.debug('here ' + JSON.stringify(result.envelope.body.fetchnextresult.return.rowset));
        callback(null,JSON.stringify(result.envelope.body.fetchnextresult.return.rowset),JSON.stringify(result.envelope.body.fetchnextresult.return.finished));
        return;
      }
    });

  });

};

/** Sanitises XML, removing special characters. */
function sanitiseXML(xml) {
  xml = xml.replace(/\x00/g, ''); // Remove any hexadecimal null characters
  xml = xml.replace(/&shy;/g, '');
  return xml;
}

/** Sanitise string - replace special characters with their safe SOAP XML equivalents. */
function sanitiseForXML(str) {
  str = str.replace(/&/g, '&amp;');
  str = str.replace(/</g, '&lt;');
  str = str.replace(/>/g, '&gt;');
  str = str.replace(/\\/g, '&apos;');
  str = str.replace(/"/g, '&quot;');
  return str;
}