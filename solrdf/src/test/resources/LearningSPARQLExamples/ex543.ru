# filename: ex543.ru

PREFIX d:  <http://learningsparql.com/ns/data#>
PREFIX dm: <http://learningsparql.com/ns/demo#>

INSERT 
{ GRAPH d:g1
  { d:x dm:tag "three" }
}
WHERE {}
