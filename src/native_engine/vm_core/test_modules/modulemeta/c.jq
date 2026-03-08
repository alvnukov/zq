module {whatever:null};
import "a" as foo;
import "d" as d {search:"./"};
import "d" as d2 {search:"./"};
import "data" as $d;

def a: 0;
def c:
  if $d::d[0] != {this:"is a test",that:"is too"} then error("data import is busted")
  elif d2::meh != d::meh then error("import twice doesn't work")
  elif foo::a != "a" then error("foo::a didn't work as expected")
  elif d::meh != "meh" then error("d::meh didn't work as expected")
  else foo::a + "c" + d::meh
  end;
