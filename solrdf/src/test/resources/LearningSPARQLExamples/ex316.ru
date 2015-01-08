# filename: ex316.ru

PREFIX ab: <http://learningsparql.com/ns/addressbook#>

INSERT
{ ?person a ab:Person . }
WHERE 
{
  ?person ab:firstName ?firstName ;
          ab:lastName  ?lastName . 
}

