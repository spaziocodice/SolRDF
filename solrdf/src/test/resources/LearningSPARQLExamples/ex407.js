// filename: ex407.js: Convert JSON results of SPARQL query about
// wind powercompanies to HTML. Reading of disk file based on case 
// conversion demo at http://en.wikipedia.org/wiki/Rhino_(JavaScript_engine)

importPackage(java.io);      // for BufferedReader 
importPackage(java.lang);    // for System[]

// Read the file into the string s
 
var reader = new BufferedReader( new InputStreamReader(System['in']) );
 
var s = true;
var result = "";
 
while (s) {
    s = reader.readLine();
    if (s != null) {
        result = result + s;
    }
};

// Parse the string into a JSON object

var JSONObject = JSON.parse(result);

// Output the values stored in the JSON object as an HTML table.

print("<html><head>");
print("<style type='text/css'>* { font-family: arial,helvetica; }</style>");
print("</head><body>");
print("<table border='1' style='border: 1px solid; border-collapse: collapse;'>");
print("<tr><th>Name</th><th>Description</th></td>");

// Make each company name a link to their homepage. 

for (i in JSONObject.results.bindings) {
    print("<tr><td><a href='");
    print(JSONObject.results.bindings[i].homepage.value);
    print("'>");
    print(JSONObject.results.bindings[i].name.value);
    print("</a></td><td>");
    print(JSONObject.results.bindings[i].description.value);
    print("</td></tr>");
}

print("</table></body></html>");
