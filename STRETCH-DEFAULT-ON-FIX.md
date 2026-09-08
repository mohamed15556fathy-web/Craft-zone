# Stretch Default ON Fix

- `add_order.html`: each new item starts with `has_stretch` enabled.
- `calculator.html`: new calculations start with `has_stretch` enabled.
- Calculator URL-prefill without an explicit `has_stretch` value keeps the default enabled.
- Loading an existing saved order still respects its stored `has_stretch` value.
