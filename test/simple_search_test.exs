defmodule SimpleSearchTest do
  use ExUnit.Case
  doctest SimpleSearch

  test "greets the world" do
    SimpleSearch.index_new(:test, %{"firstname" => [], "lastname" => [], "location" => []})

    SimpleSearch.index_add_doc(:test, "123", %{
      "firstname" => "John",
      "lastname" => "Doe",
      "location" => "Los Angeles"
    })

    SimpleSearch.index_add_doc(:test, "124", %{
      "firstname" => "Jane",
      "lastname" => "Doe",
      "location" => "New York City"
    })

    assert %{"123" => [{"firstname", "john"}], "124" => [{"firstname", "jane"}]} ==
             SimpleSearch.search_index(:test, "J")

    assert %{"123" => [{"firstname", "john"}]} ==
             SimpleSearch.search_index(:test, "Jo")

    assert %{"124" => [{"location", "york"}]} ==
             SimpleSearch.search_index(:test, "York")

    SimpleSearch.index_remove_doc(:test, "124")

    assert %{"123" => [{"firstname", "john"}]} ==
             SimpleSearch.search_index(:test, "J")
  end
end
