# filename: ex484.ru

PREFIX dm:  <http://learningsparql.com/ns/demo#> 
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 

DELETE 
{ ?s dm:amount ?amount }
INSERT
{ ?s dm:amount ?integerAmount }
WHERE 
{ 
  ?s dm:amount ?amount .
  BIND (xsd:integer(?amount) AS ?integerAmount )
}
  