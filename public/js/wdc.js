(function() {
    // Create the connector object

    var myConnector = tableau.makeConnector();
    var sessObj = {'sessionid':'','reportPath':''};

    myConnector.shutdown = function(shutdownCallback) {
		if(tableau.phase === 'gatherData') {
            $.post("/logoff", JSON.parse(tableau.connectionData), function(data, status) {
                tableau.log('WDC Shutdown - ' + status + ' OBI Session Obj - ' + tableau.connectionData);
            });
        }
        shutdownCallback();
	};


    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        tableau.log('Get schema! - ' + tableau.phase + ' phase OBI Session Obj ' + JSON.parse(tableau.connectionData));

            $.post("/getSchema", JSON.parse(tableau.connectionData), function(data, status) {

                cols = [];
                var i;
                for (i = 0; i < data.length; i++) {
                    cols.push({
                        "id":data[i].id,
                        "dataType":eval(data[i].dataType)
                    });
                }

                var tableSchema = {
                    id: "FRCSchema",
                    alias: "FRC Schema",
                    columns: cols
                };


                schemaCallback([tableSchema]);
            });

    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        tableau.log('Get Data - ' + tableau.phase + ' phase OBI Session Obj ' + JSON.parse(tableau.connectionData));      
        $.post("/getData", JSON.parse(tableau.connectionData), function(data, status) {
            try {
                var out=data.map(function(el){
                    var arr=[];
                    for(var key in el){
                      arr.push(el[key]);
                    }
                    return arr;
                  });

                table.appendRows(out);

                doneCallback();
            } catch(error) {
                tableau.abortWithError(JSON.stringify(data));
            }
        },'json');

    };

    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {

        $("#submitButton").click(function() {
            sessObj.reportPath = document.getElementById("reportPath").value;
            tableau.connectionData = JSON.stringify(sessObj);
            tableau.connectionName="FRC WDC";
            tableau.submit();

        });


        $("#signInButton").click(function() {

            $.post("/login", $("#loginForm").serialize() ,function(data,status){
                console.log(status);
                
                document.getElementById("loginForm").style.display="none";
                document.getElementById("spinner").style.display="block";
                
                if(status == 'success') {
                    $.post("/OBIlogon",{}, function(data, status) {
                        sessObj.sessionid = data;
                        tableau.connectionData = JSON.stringify(sessObj);
                        tableau.log('WDC Interactive phase OBI Session Obj - ' + tableau.connectionData);
                        document.getElementById("spinner").style.display="none";
                        document.getElementById("extractDiv").style.display="block";
                    });
                } else {

                }


            });

        });


    });




})();
