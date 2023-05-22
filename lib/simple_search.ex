defmodule SimpleSearch do
  @moduledoc """
  Documentation for `SimpleSearch`.
  """
  @stop_words_english File.read!("priv/english.txt") |> String.split()
  @stop_words_german File.read!("priv/german.txt") |> String.split()
  @stop_words @stop_words_english ++ @stop_words_german

  def index_new(idx_name, config) when is_atom(idx_name) and is_map(config) do
    :ets.new(idx_name, [:named_table, :public])

    Enum.each(config, fn {field_name, _field_config} ->
      t = :ets.new(idx_name, [:public, :ordered_set])
      :ets.insert(idx_name, {field_name, t})
    end)
  end

  def index_add_doc(idx_name, doc_id, doc) when is_atom(idx_name) and is_map(doc) do
    :ets.foldl(
      fn {field_name, t}, _acc ->
        Map.get(doc, field_name, "")
        |> split()
        |> reject_stop_words()
        |> Enum.each(fn value ->
          :ets.insert(t, {{value |> String.downcase(), doc_id}})
        end)
      end,
      :ignored_acc,
      idx_name
    )

    :ok
  end

  def index_remove_doc(idx_name, doc_id) when is_atom(idx_name) do
    :ets.foldl(
      fn {_field_name, t}, _acc ->
        :ets.match_delete(t, {{:_, doc_id}})
      end,
      :ignored_acc,
      idx_name
    )

    :ok
  end

  def search(idx_name, field_search, string_search) do
    results =
      search_fields([], idx_name, field_search)
      |> search_free_text(idx_name, string_search)

    case results do
      :no_result ->
        %{}

      _ ->
        results
        |> List.flatten()
        |> Enum.group_by(fn {doc_id, _} -> doc_id end, fn {_, v} -> v end)
    end
  end

  defp search_fields(results, idx_name, field_search, default \\ :no_result)

  defp search_fields(
         results,
         idx_name,
         [{field_name, search_string} | rest],
         default
       ) do
    case :ets.lookup(idx_name, field_name) do
      [{_field_name, t}] ->
        search_string
        |> split()
        |> Enum.reduce(results, fn
          _, :no_result ->
            default

          search_substring, results_acc ->
            search_substring = String.downcase(search_substring)

            res = search_field_index_(t, field_name, search_substring)

            if res == [] do
              default
            else
              search_fields([res | results_acc], idx_name, rest, default)
            end
        end)

      _ ->
        search_fields(results, idx_name, rest, default)
    end
  end

  defp search_fields(results, _idx_name, [], _default), do: results

  defp search_free_text(results, _, ""), do: results
  defp search_free_text(:no_result, _, _), do: :no_result

  defp search_free_text(results, idx_name, search_string) do
    free_text_results =
      :ets.foldl(
        fn
          {field_name, _t}, results_acc ->
            search_fields(results_acc, idx_name, [{field_name, search_string}], results_acc)
        end,
        [],
        idx_name
      )

    case free_text_results do
      [] -> :no_result
      _ -> free_text_results ++ results
    end
  end

  defp split(field_value) when is_binary(field_value) do
    String.split(field_value, [" ", "-", "/", "_", "."])
  end

  defp split(field_value) when is_list(field_value) do
    Enum.flat_map(field_value, fn fv -> split(fv) end)
  end

  defp split(field_value) when is_map(field_value) do
    split(Map.values(field_value))
  end

  defp split(_field_value) do
    []
  end

  defp reject_stop_words(field_values) do
    Enum.reject(field_values, fn v ->
      v in @stop_words
    end)
  end

  defp search_field_index_(t, field_name, search_substring) do
    search_field_index_(t, field_name, {search_substring, :_}, search_substring, [])
  end

  defp search_field_index_(t, field_name, key, search_substring, acc) do
    case :ets.next(t, key) do
      :"$end_of_table" ->
        acc

      {key_string, doc_id} = key ->
        if String.starts_with?(key_string, search_substring) do
          search_field_index_(t, field_name, key, search_substring, [
            {doc_id, {field_name, key_string}} | acc
          ])
        else
          acc
        end
    end
  end
end
