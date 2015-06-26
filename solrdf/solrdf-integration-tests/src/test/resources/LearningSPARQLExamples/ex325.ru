# filename: ex325.ru

PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ab: <http://learningsparql.com/ns/addressbook#> 

DELETE  
{ ?s ab:email ?o }
INSERT 
{ ?s foaf:mbox ?o }
WHERE 
{?s ab:email ?o }

