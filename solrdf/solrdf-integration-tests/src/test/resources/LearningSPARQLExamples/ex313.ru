# filename: ex313.ru

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ab:   <http://learningsparql.com/ns/addressbook#>
PREFIX d:    <http://learningsparql.com/ns/data#> 

INSERT
{ 
  d:i8301 ab:homeTel "(718) 440-9821" . 
  ab:Person a rdfs:Class .
}
WHERE {}

