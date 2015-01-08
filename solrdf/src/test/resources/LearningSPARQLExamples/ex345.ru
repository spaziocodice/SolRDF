# filename: ex345.ru

PREFIX d:  <http://learningsparql.com/ns/data#>
PREFIX dm: <http://learningsparql.com/ns/demo#>

INSERT 
{ GRAPH d:g4
  { ?s dm:tag "five", "six" . } 
}
USING NAMED d:g2
WHERE
{ GRAPH d:g2 
  {
    ?s dm:tag  "five" . 
    ?s dm:tag  "six" . 
  }
}

