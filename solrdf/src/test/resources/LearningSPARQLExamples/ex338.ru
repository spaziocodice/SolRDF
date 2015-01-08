# filename: ex338.ru

PREFIX d:  <http://learningsparql.com/ns/data#>
PREFIX dm: <http://learningsparql.com/ns/demo#>

INSERT DATA
{
  d:x dm:tag "one" . 
  d:x dm:tag "two" . 

  GRAPH d:g1
  { 
    d:x dm:tag "three" . 
    d:x dm:tag "four" . 
  }

  GRAPH d:g2
  { 
    d:x dm:tag "five" . 
    d:x dm:tag "six" . 
  }
}

