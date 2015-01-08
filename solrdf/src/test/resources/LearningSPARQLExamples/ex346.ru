# filename: ex346.ru

PREFIX d:  <http://learningsparql.com/ns/data#>
PREFIX dm: <http://learningsparql.com/ns/demo#>

DELETE DATA
{ GRAPH d:g2
  { d:x dm:tag "six" } 
}

