# filename: ex334.ru

PREFIX d:  <http://learningsparql.com/ns/data#>
PREFIX dm: <http://learningsparql.com/ns/demo#>

INSERT DATA
{ GRAPH d:g2
  { 
    d:x dm:tag "five" . 
    d:x dm:tag "six" . 
  }
}

