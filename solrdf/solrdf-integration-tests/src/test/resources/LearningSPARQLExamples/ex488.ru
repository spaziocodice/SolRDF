# filename: ex488.ru

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ab:   <http://learningsparql.com/ns/addressbook#>
PREFIX d:    <http://learningsparql.com/ns/data#> 
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT 
{
   d:Student a rdfs:Class . 
   d:Course a rdfs:Class . 
   ?student a d:Student . 
   ?course a d:Course . 
}
WHERE {

  ?student ab:firstName ?fn ;
           ab:lastName ?ln . 

  ?course  ab:courseTitle ?ct . 
}
