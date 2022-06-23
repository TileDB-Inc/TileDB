
template <class Reference>
struct arrow_proxy {
  Reference  r;
  Reference* operator->() { return &r; }
};
