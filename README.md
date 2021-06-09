# Beeline

![Actions CI](https://github.com/NFIBrokerage/beeline/workflows/Actions%20CI/badge.svg)

a tool for building in-order GenStage topologies for EventStoreDB

## Installation

```elixir
def deps do
  [
    {:beeline, "~> 0.1"},
    # these are optional: add whichever adapter your project needs
    {:kelvin, "~> 0.3"},
    {:volley, "~> 0.4"}
  ]
end
```

Check out the docs here: https://cuatro.hexdocs.pm/beeline
