# filename: ex481.ru

PREFIX ab: <http://learningsparql.com/ns/addressbook#> 
PREFIX v:  <http://www.w3.org/2006/vcard/ns#>

DELETE 
{
 ?s ab:firstName  ?firstName ;
    ab:lastName   ?lastName ;
    ab:email      ?email .
    ?s ab:homeTel ?homeTel . 
}
INSERT
{
 ?s v:given-name  ?firstName ;
    v:family-name ?lastName ;
    v:email       ?email ;
    v:homeTel     ?homeTel . 
}
WHERE
{
 ?s ab:firstName ?firstName ;
    ab:lastName  ?lastName ;
    ab:email     ?email .
    OPTIONAL 
    { ?s ab:homeTel ?homeTel . }
}
