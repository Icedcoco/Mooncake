#include "window.h"
#include <cassert>
int main() {
    LiveWindow w(3);
    w.Add("a");
    w.Add("b");
    assert(w.Due(2).empty());
    w.Add("c");
    assert(w.Due(2) == (std::vector<std::string>{"a", "b"}));
    w.Complete({false, true});
    assert(w.size() == 2);
    w.Add("d");
    assert(w.Due(2) == (std::vector<std::string>{"a", "c"}));
    w.Complete({true, true});
    assert(w.size() == 1);
}
