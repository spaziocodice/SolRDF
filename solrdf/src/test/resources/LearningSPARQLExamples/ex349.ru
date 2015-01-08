# filename: ex349.ru

PREFIX d: <http://learningsparql.com/ns/data#>

DELETE 
{ GRAPH d:g2 { ?s ?p "five" } }
INSERT
{ GRAPH d:g2 { ?s ?p "cinco" } }
WHERE
{ GRAPH d:g2 { ?s ?p "five" } }

