# filename: ex329.ru

PREFIX skos: <http://www.w3.org/2004/02/skos/core#> 
PREFIX xl:   <http://www.w3.org/2008/05/skos-xl#> 

DELETE
{ ?concept skos:prefLabel ?labelString . }
INSERT
{
  ?newURI a xl:Label ; 
          xl:literalForm ?labelString . 
  ?concept xl:prefLabel ?newURI . 
}
WHERE 
{ 
  ?concept skos:prefLabel ?labelString .
  BIND (URI(CONCAT("http://learningsparql.com/ns/data#",
                    ENCODE_FOR_URI(str(?labelString)))
           ) AS ?newURI)
}

