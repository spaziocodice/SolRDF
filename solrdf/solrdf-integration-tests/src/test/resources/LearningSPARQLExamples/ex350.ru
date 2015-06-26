# filename: ex350.ru

PREFIX d: <http://learningsparql.com/ns/data#>

WITH d:g2
DELETE 
{ ?s ?p "five" }
INSERT
{ ?s ?p "cinco" } 
WHERE
{ ?s ?p "five" }

