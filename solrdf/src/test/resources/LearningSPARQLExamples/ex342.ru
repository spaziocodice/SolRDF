# filename: ex342.ru

PREFIX d:  <http://learningsparql.com/ns/data#>
PREFIX dm: <http://learningsparql.com/ns/demo#>

WITH d:g2
INSERT 
{ 
  d:x dm:tag "five" .
  d:x dm:tag "six" .
}
WHERE {}

