import marimo

__generated_with = "0.20.2"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Top 10 Authors by Book Count

    This notebook uses **dlt** to access the Open Library pipeline data
    and **ibis** to query and aggregate the results.
    """)
    return


@app.cell
def _():
    import dlt
    import ibis

    pipeline = dlt.attach(pipeline_name="open_library_pipeline")
    dataset = pipeline.dataset()
    return dataset, ibis


@app.cell
def _(dataset, ibis):
    authors_tbl = dataset["books__authors"].to_ibis()
    books_tbl = dataset["books"].to_ibis()

    top_authors = (
        authors_tbl.join(books_tbl, authors_tbl._dlt_parent_id == books_tbl._dlt_id)
        .group_by(authors_tbl.name)
        .agg(book_count=authors_tbl.name.count())
        .order_by(ibis.desc("book_count"))
        .limit(10)
        .execute()
        .reset_index(drop=True)
    )

    top_authors
    return (top_authors,)


@app.cell
def _(mo, top_authors):
    import altair as alt

    chart = (
        alt.Chart(top_authors)
        .mark_bar(color="#4C78A8")
        .encode(
            x=alt.X(
                "book_count:Q",
                title="Book Count",
                axis=alt.Axis(tickMinStep=1),
            ),
            y=alt.Y("name:N", sort="-x", title="Author"),
            tooltip=["name", "book_count"],
        )
        .properties(
            title="Top 10 Authors by Book Count",
            width=600,
            height=300,
        )
    )

    mo.ui.altair_chart(chart)
    return


@app.cell(hide_code=True)
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
