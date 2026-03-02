import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import dlt
    import ibis
    import plotly.express as px

    return dlt, ibis, px


@app.cell
def _(dlt):
    # Attach to the existing taxi_pipeline and get an ibis connection
    pipeline = dlt.attach(pipeline_name="taxi_pipeline")
    dataset = pipeline.dataset()
    # Expose the connection to marimo's data sources sidebar
    con = dataset.ibis()
    return (dataset,)


@app.cell
def _(dataset, ibis):
    # Build aggregation using ibis
    rides = dataset.table("rides").to_ibis()
    payment_counts = (
        rides
        .group_by("payment_type")
        .aggregate(count=rides.payment_type.count())
        .order_by(ibis.desc("count"))
    )
    df = dataset(payment_counts).df()
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(df, px):
    fig = px.pie(
        df,
        names="payment_type",
        values="count",
        title="NYC Taxi Rides by Payment Type",
        hole=0.3,
    )
    fig.update_traces(textinfo="percent+label")
    fig
    return


if __name__ == "__main__":
    app.run()
