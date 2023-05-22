defmodule SimpleSearchTest do
  use ExUnit.Case
  doctest SimpleSearch

  test "greets the world" do
    SimpleSearch.index_new(:test, %{"firstname" => [], "lastname" => [], "location" => []})

    SimpleSearch.index_add_doc(:test, "123", %{
      "firstname" => "John",
      "lastname" => "Doe",
      "location" => "Jbay"
    })

    SimpleSearch.index_add_doc(:test, "124", %{
      "firstname" => "Jane",
      "lastname" => "Doe",
      "location" => "New York City"
    })

    assert %{
             "123" => [{"location", "jbay"}, {"firstname", "john"}],
             "124" => [{"firstname", "jane"}]
           } ==
             SimpleSearch.search(:test, [], "J")

    assert %{"123" => [{"firstname", "john"}]} ==
             SimpleSearch.search(:test, [], "Jo")

    assert %{"124" => [{"location", "york"}]} ==
             SimpleSearch.search(:test, [], "York")

    assert %{"123" => [{"location", "jbay"}]} ==
             SimpleSearch.search(:test, [{"location", "JBa"}], "")

    assert %{} == SimpleSearch.search(:test, [], "Jane Doe Manhatten")
    assert %{} == SimpleSearch.search(:test, [{"location", "JBay"}], "South Africa")

    SimpleSearch.index_remove_doc(:test, "124")

    assert %{"123" => [{"location", "jbay"}, {"firstname", "john"}]} ==
             SimpleSearch.search(:test, [], "J")
  end
end
