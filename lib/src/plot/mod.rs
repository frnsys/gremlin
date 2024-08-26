//! Common charts for looking at data.
//!
//! If more charts are needed, refer to:
//!
//! - <https://echarts.apache.org/examples/en/index.html>
//! - <https://github.com/yuankunzhang/charming/tree/main/gallery>
//!
//! # Example usage
//!
//! ```
//! StaticChart::timeseries(
//!     vec![
//!         (
//!             "Wind".into(),
//!             vec![1., 2., 3., 4.],
//!         ),
//!         (
//!             "Solar".into(),
//!             vec![1., 2., 3., 4.],
//!         )
//!     ],
//!     &["Morning", "Day", "Evening", "Night"], // X-axis labels
//!     "%", // Units
//! )
//! .with_titles(
//!     "Estimated Solar & Wind Capacity Factors",
//!     "Wind: PLUSWIND; Solar: LBNL",
//! )
//! .render((1200, 800), Path::new("solar_wind_cfs.png"));
//! ```

use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, LazyLock, Mutex},
    thread,
};

use ahash::HashMap;
pub use charming::element::{LineStyleType, Symbol};
use charming::{
    component::{Axis, Grid, Legend, Title, VisualMap},
    datatype::{
        CompositeValue,
        DataFrame,
        DataPoint,
        DataSource,
        Dataset,
        NumericValue,
        Transform,
    },
    df,
    element::{
        AxisLabel,
        AxisPointer,
        AxisType,
        DimensionEncode,
        Emphasis,
        ItemStyle,
        Label,
        LineStyle,
        MarkLine,
        MarkLineData,
        MarkLineVariant,
        NameLocation,
        Orient,
        SplitArea,
        SplitLine,
        TextStyle,
    },
    series::{Bar, Boxplot, Heatmap, Line, Scatter},
    theme::Theme,
    Chart,
    ImageRenderer,
};
use image::{
    imageops::{crop, overlay},
    ImageFormat,
    Rgba,
    RgbaImage,
};

const COLORS: &[&str] = &[
    "#4992ff", "#7cffb2", "#fddd60", "#ff6e76", "#58d9f9", "#05c091",
    "#ff8a45", "#8143cf", "#dd79ff", "#4a698c", "#e9d8a6",
];

/// HACK: A wrapper to create a singleton `ImageRenderer`.
///
/// Rendering plots to images concurrently will result in a segfault, due to
/// <https://github.com/denoland/deno_core/issues/150>,
/// so we use this wrapper to ensure that only one instance of `ImageRenderer` exists.
pub struct SafeImageRenderer {
    sender: mpsc::Sender<((u32, u32), Chart)>,
    receiver: mpsc::Receiver<String>,
}
impl Default for SafeImageRenderer {
    fn default() -> Self {
        let (command_sender, command_receiver) =
            mpsc::channel::<((u32, u32), Chart)>();
        let (response_sender, response_receiver) = mpsc::channel();
        thread::spawn(move || {
            for (size, chart) in command_receiver {
                let mut renderer = ImageRenderer::new(size.0, size.1)
                    .theme(Theme::Dark);
                let svg_data = renderer.render(&chart).unwrap();
                response_sender.send(svg_data).unwrap();
            }
        });
        SafeImageRenderer {
            sender: command_sender,
            receiver: response_receiver,
        }
    }
}
impl SafeImageRenderer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn render_chart_svg(size: (u32, u32), chart: Chart) -> String {
        let renderer = IMAGE_RENDERER.lock().unwrap();
        renderer.sender.send((size, chart)).unwrap();
        renderer.receiver.recv().unwrap()
    }
}
static IMAGE_RENDERER: LazyLock<Arc<Mutex<SafeImageRenderer>>> =
    LazyLock::new(|| Arc::new(Mutex::new(SafeImageRenderer::new())));

/// `Vec` of `(group_name, values)`.
type TimeSeriesGroup<T> = Vec<(String, Vec<T>)>;

/// `Vec` of `(group_name, values)`.
type BoxPlotGroup<T> = Vec<(String, Vec<T>)>;

/// A single set of [`ScatterItem`]s that should
/// be grouped together in a single subplot of a
/// [`StaticChart::cat_scatter`] plot.
#[derive(Default)]
pub struct CatScatterItem<'a, T> {
    pub var: String,
    pub units: &'a str,
    pub groups: Vec<ScatterItem<T>>,
}

/// A group of data which should be e.g. colored
/// the same in a scatter plot.
#[derive(Default)]
pub struct ScatterItem<T> {
    /// The name used to label these points.
    pub name: String,

    /// Optionally override the default symbol (a circle).
    pub symbol: Option<Symbol>,

    /// The actual values to plot.
    pub values: Vec<T>,
}
impl<'a, T> From<(&'a str, Vec<T>)> for ScatterItem<T> {
    fn from((name, values): (&'a str, Vec<T>)) -> Self {
        ScatterItem {
            name: name.to_string(),
            symbol: None,
            values,
        }
    }
}
impl<T> From<(String, Vec<T>)> for ScatterItem<T> {
    fn from((name, values): (String, Vec<T>)) -> Self {
        ScatterItem {
            name,
            symbol: None,
            values,
        }
    }
}

type CatScatterGroup<'a, T> = Vec<CatScatterItem<'a, T>>;

/// Used to display static reference lines,
/// e.g. for comparing against reference data.
pub enum LineMarker {
    Vertical(String, f32),
    Horizontal(String, f32),
}

/// Represents styling for a specific
/// plot element (e.g. a line).
pub struct ElemStyle {
    pub color: &'static str,
    pub symbol: Symbol,
    pub line: LineStyleType,
    pub width: f32,
    pub symbol_size: f32,
}
impl Default for ElemStyle {
    fn default() -> Self {
        Self {
            color: "#ffffff",
            symbol: Symbol::Circle,
            line: LineStyleType::Solid,
            width: 1.,
            symbol_size: 8.,
        }
    }
}

/// This trait can be implemented to pass to [`StaticChart`]
/// for control over some of the plot styling.
pub trait Style {
    /// Get a style for an element by index.
    fn get_style(label: &str, i: usize) -> Option<ElemStyle>;
}
impl Style for () {
    fn get_style(_label: &str, _i: usize) -> Option<ElemStyle> {
        None
    }
}

/// A higher-level chart to easily produce certain types of plots.
pub struct StaticChart<S: Style = ()> {
    chart: Chart,

    /// An optional chart that includes
    /// only the legend, for when we need to render it
    /// separately to avoid overlap.
    /// See: <https://github.com/apache/echarts/issues/14252>
    legend: Option<Chart>,

    marker: PhantomData<S>,
}
impl<S: Style> StaticChart<S> {
    /// Common chart configuration.
    fn base_chart() -> Chart {
        Chart::new()
            .background_color("#121212")
            .legend(Legend::new().top("bottom"))
    }

    /// A boxplot. Data is expected to be a `Vec` of `(label, values)`.
    pub fn boxplot<T: Into<NumericValue>>(
        data: BoxPlotGroup<T>,
        units: &str,
        horizontal: bool,
    ) -> Self
    where
        DataSource: From<Vec<Vec<T>>>,
    {
        let (labels, values): (Vec<_>, Vec<_>) =
            data.into_iter().unzip();
        let transform = format!(
            r#"{{ "type": "boxplot", "config": {{ "itemNameFormatter": ({{value}}) => {:?}[value] }} }}"#,
            labels
        );
        let ds = Dataset::new()
            .source(values)
            .transform(transform.as_str())
            .transform(
                Transform::new()
                    .from_dataset_index(1)
                    .from_transform_result(1),
            );

        let group_axis = Axis::new()
            .type_(AxisType::Category)
            .boundary_gap(true)
            .name_gap(30)
            .axis_label(
                AxisLabel::new()
                    .formatter("{value}")
                    // Ensure all category labels are shown.
                    .interval(0),
            )
            .split_area(SplitArea::new().show(false))
            .split_line(SplitLine::new().show(false));
        let value_axis = Axis::new()
            .type_(AxisType::Value)
            .max("dataMax")
            .axis_label(
                AxisLabel::new()
                    .formatter(format!("{{value}}{units}").as_str()),
            )
            .split_area(SplitArea::new().show(true));
        let mut outliers =
            Scatter::new().name("Outliers").dataset_index(2);

        let (x_axis, y_axis) = if horizontal {
            outliers =
                outliers.encode(DimensionEncode::new().x(1).y(0));
            (value_axis, group_axis)
        } else {
            (group_axis, value_axis)
        };

        let chart = StaticChart::<S>::base_chart()
            // Ensure labels aren't cut off
            .grid(Grid::new().contain_label(true))
            .dataset(ds)
            .y_axis(y_axis)
            .x_axis(x_axis)
            .series(Boxplot::new().name("Boxplots").dataset_index(1))
            .series(outliers);

        StaticChart {
            chart,
            legend: None,
            marker: PhantomData,
        }
    }

    /// Plots multiple scatter plots, where each subplot shows one set of
    /// values for different groups.
    pub fn cat_scatter(
        data: CatScatterGroup<f32>,
        horizontal: bool,
    ) -> Self {
        let mut chart = StaticChart::<S>::base_chart();

        // Have to do some manual layout.
        let n_groups = data.len();
        let margin = 5.;
        let sub_width = (100. - (margin * 2.)) / n_groups as f32;

        for (i, item) in data.into_iter().enumerate() {
            // Figure out offset from the right.
            let right = margin + (i as f32) * sub_width;

            let CatScatterItem { var, units, groups } = item;
            let group_axis = Axis::new()
                .type_(AxisType::Category)
                .grid_index(i as f64)
                .name(var)
                .axis_label(AxisLabel::new().show(false))
                .name_location(NameLocation::Center);
            let value_axis =
                Axis::new()
                    .type_(AxisType::Value)
                    .grid_index(i as f64)
                    .axis_label(AxisLabel::new().formatter(
                        format!("{{value}}{units}").as_str(),
                    ));

            let (x_axis, y_axis) = if horizontal {
                (value_axis, group_axis)
            } else {
                (group_axis, value_axis)
            };

            chart = chart
                .grid(
                    // Here a "grid" is essentially a subplot.
                    Grid::new()
                        .right(format!("{}%", right + margin))
                        .contain_label(true)
                        .width(format!(
                            "{}%",
                            sub_width - (margin * 2.)
                        )),
                )
                .x_axis(x_axis)
                .y_axis(y_axis);

            for (j, group) in groups.into_iter().enumerate() {
                let ScatterItem {
                    name,
                    symbol,
                    values,
                } = group;

                let n_values = values.len() as f32;
                let data = values
                    .into_iter()
                    .enumerate()
                    .map(|(i, val)| {
                        DataPoint::from(CompositeValue::from(vec![
                            j as f32 + i as f32 / n_values,
                            val,
                        ]))
                    })
                    .collect::<Vec<_>>();
                chart = chart.series(
                    Scatter::new()
                        .name(name)
                        .symbol(symbol.unwrap_or(Symbol::Circle))
                        // .symbol_size(2.5)
                        .x_axis_index(i as f64)
                        .y_axis_index(i as f64)
                        .data(data),
                );
            }
        }

        StaticChart {
            chart,
            legend: None,
            marker: PhantomData,
        }
    }

    /// Line plots of time series. Data is expected to be a `Vec` of `(label, values)`.
    ///
    /// - `x_axis_labels` should be a slice of strings that align with each time-step (e.g. a datetime).
    ///
    /// # Notes
    ///
    /// Legend rendering here is handled in a hacky way to ensure it won't overlap
    /// the chart; see the note on [`StaticChart::legend`].
    pub fn timeseries<
        T: Into<NumericValue> + Clone,
        L: Into<String> + Clone,
    >(
        data: TimeSeriesGroup<T>,
        x_axis_labels: &[L],
        units: &str,
    ) -> Self {
        let mut chart = StaticChart::<S>::base_chart()
            // Hide the legend, we will render it separately.
            .legend(Legend::new().show(false))
            // Ensure labels aren't cut off
            .grid(Grid::new().contain_label(true))
            .x_axis(
                Axis::new()
                    .type_(AxisType::Category)
                    .boundary_gap(false)
                    .data(x_axis_labels.to_vec()),
            )
            .y_axis(
                Axis::new()
                    .type_(AxisType::Value)
                    .axis_label(AxisLabel::new().formatter(
                        format!("{{value}}{units}").as_str(),
                    ))
                    .axis_pointer(AxisPointer::new().snap(true)),
            );

        // Prepare the legend as a separate chart.
        let mut legend = StaticChart::<S>::base_chart()
            .legend(
                Legend::new()
                    .left(0)
                    .right(0)
                    .top(0)
                    .bottom(0)
                    .item_gap(16.)
                    .text_style(TextStyle::new().font_size(15.))
                    .height("100%"),
            )
            .grid(Grid::new())
            .x_axis(Axis::new().show(false))
            .y_axis(Axis::new().type_(AxisType::Value).show(false));

        // Render the lines and the legend.
        for (i, (label, values)) in data.into_iter().enumerate() {
            let style =
                S::get_style(&label, i).unwrap_or_else(|| get_style(i));
            chart = chart.series(
                Line::new()
                    .name(label.clone())
                    .smooth(0.5)
                    .symbol(style.symbol)
                    .symbol_size(style.symbol_size)
                    .item_style(ItemStyle::new().color(style.color))
                    .line_style(
                        LineStyle::new()
                            .color(style.color)
                            .type_(style.line)
                            .width(style.width),
                    )
                    .data::<T>(values),
            );

            // For the legend we render empty datasets
            // so that only the legend shows up.
            let style =
                S::get_style(&label, i).unwrap_or_else(|| get_style(i));
            legend = legend.series(
                Line::new()
                    .name(label)
                    .smooth(0.5)
                    .symbol(style.symbol)
                    .symbol_size(style.symbol_size)
                    .item_style(ItemStyle::new().color(style.color))
                    .line_style(
                        LineStyle::new()
                            .color(style.color)
                            .type_(style.line),
                    )
                    .data::<f32>(vec![]),
            );
        }

        StaticChart {
            chart,
            legend: Some(legend),
            marker: PhantomData,
        }
    }

    /// A simple bar chart.
    pub fn bar<L: Into<String> + Clone>(
        data: Vec<f32>,
        x_axis_labels: &[L],
        units: &str,
        min: f32,
        max: f32,
    ) -> Self {
        let mut chart = StaticChart::<S>::base_chart()
            // Ensure labels aren't cut off
            .grid(Grid::new().contain_label(true).top(10.).bottom(10.))
            .x_axis(
                Axis::new()
                    .type_(AxisType::Category)
                    .boundary_gap(false)
                    .data(x_axis_labels.to_vec()),
            )
            .y_axis(
                Axis::new()
                    .min(min)
                    .max(max)
                    .type_(AxisType::Value)
                    .axis_label(AxisLabel::new().formatter(
                        format!("{{value}}{units}").as_str(),
                    ))
                    .axis_pointer(AxisPointer::new().snap(true)),
            );

        // Render the bars and the legend.
        chart = chart.series(Bar::new().bar_width(20.).data(data));

        StaticChart {
            chart,
            legend: None,
            marker: PhantomData,
        }
    }

    /// A correlation matrix plot, exhaustively displaying
    /// pairwise relationships between columns.
    ///
    /// - `cols` are the column names/labels
    /// - `data` is a list of `(column index, column index, value)`
    pub fn correlation<L: Into<String> + Clone>(
        cols: &[L],
        data: Vec<(usize, usize, f32)>,
    ) -> Self {
        let data: Vec<DataFrame> = data
            .into_iter()
            .map(|d| {
                df![
                    d.1 as f32,
                    d.0 as f32,
                    if d.2 == 0. {
                        CompositeValue::from("-")
                    } else {
                        // Hack to limit decimal places/avoid float precision issues
                        CompositeValue::from(
                            f64::trunc(d.2 as f64 * 100.0) / 100.0,
                        )
                    }
                ]
            })
            .collect();

        let chart = StaticChart::<S>::base_chart()
            .grid(Grid::new().height("50%").top("10%"))
            .x_axis(
                Axis::new()
                    .type_(AxisType::Category)
                    .data(cols.to_vec())
                    .axis_label(AxisLabel::new().interval(0))
                    .split_area(SplitArea::new().show(true)),
            )
            .y_axis(
                Axis::new()
                    .type_(AxisType::Category)
                    .data(cols.to_vec())
                    .axis_label(AxisLabel::new().interval(0))
                    .split_area(SplitArea::new().show(true)),
            )
            .visual_map(
                VisualMap::new()
                    .min(0)
                    .max(1)
                    .calculable(true)
                    .orient(Orient::Horizontal)
                    .left("center")
                    .bottom("15%"),
            )
            .series(
                Heatmap::new()
                    .label(Label::new().show(true))
                    .emphasis(
                        Emphasis::new().item_style(
                            ItemStyle::new()
                                .shadow_blur(10)
                                .shadow_color("rgba(0, 0, 0, 0.5)"),
                        ),
                    )
                    .data(data),
            );

        StaticChart {
            chart,
            legend: None,
            marker: PhantomData,
        }
    }

    pub fn with_scatter(
        mut self,
        name: &str,
        points: &[(f32, f32)],
    ) -> Self {
        let points = points
            .iter()
            .map(|(x, y)| {
                DataPoint::from(CompositeValue::from(vec![*x, *y]))
            })
            .collect();

        self.chart = self.chart.series(
            Scatter::new()
                .name(name)
                // .symbol(symbol.unwrap_or(Symbol::Circle))
                .symbol_size(3.5)
                .item_style(ItemStyle::new().color("#EA4335"))
                .data(points),
        );
        self
    }

    pub fn with_scatters(
        mut self,
        scatters: &[(String, Vec<(f32, f32)>)],
    ) -> Self {
        for (name, points) in scatters {
            self = self.with_scatter(name, points);
        }
        self
    }

    /// Add a marker line to the chart.
    pub fn with_line(mut self, marker: LineMarker) -> Self {
        let (label, line) = match marker {
            LineMarker::Vertical(label, val) => {
                (label, MarkLineData::new().x_axis(val))
            }
            LineMarker::Horizontal(label, val) => {
                (label, MarkLineData::new().y_axis(val))
            }
        };

        // Kind of hacky, but add an empty scatter plot
        // so the line shows up correctly.
        // `Chart::mark_line` exists but doesn't seem to work?
        self.chart = self.chart.series(
            Scatter::new().mark_line(
                MarkLine::new()
                    .label(Label::new().formatter(label.as_str()))
                    .symbol(vec![Symbol::None, Symbol::None])
                    .line_style(LineStyle::new().color("#ffa500"))
                    .data(vec![MarkLineVariant::Simple(line)]),
            ),
        );

        self
    }

    /// Add a title to the chart.
    pub fn with_title(mut self, title: &str) -> Self {
        self.chart = self.chart.title(Title::new().text(title));
        self
    }

    /// Add a title *and* a subtitle to the chart.
    pub fn with_titles(mut self, title: &str, subtitle: &str) -> Self {
        self.chart = self
            .chart
            .title(Title::new().text(title).subtext(subtitle));
        self
    }

    /// Render the chart to an image.
    /// This renders at 2x DPI, by first rendering to SVG
    /// at the requested size, then scaling up 2x.
    pub fn render(self, size: (u32, u32), path: &Path) {
        let Self { chart, legend, .. } = self;
        let svg_data = SafeImageRenderer::render_chart_svg(size, chart);
        let mut img = svg_to_png(svg_data);

        // If we have a separate legend we need to combine it with the chart image.
        // Render the legend to an in-memory image, trimming
        // the whitespace/empty space.
        let legend = legend.map(|legend| {
            let svg_data =
                SafeImageRenderer::render_chart_svg(size, legend);
            let mut img = svg_to_png(svg_data);
            trim(&mut img, (18, 18, 18)) // Equivalent to #121212
        });

        if let Some(legend) = legend {
            let mut l = RgbaImage::from_pixel(
                img.width(),
                img.height() + legend.height(),
                Rgba([18, 18, 18, 255]),
            );
            overlay(&mut l, &img, 0, 0);
            overlay(&mut l, &legend, 0, img.height() as i64);
            img = l;
        }
        img.save_with_format(path, ImageFormat::Png).unwrap();
    }
}

/// Trims whitespace around an image
fn trim(img: &mut RgbaImage, empty_color: (u8, u8, u8)) -> RgbaImage {
    let mut xs = vec![];
    let mut ys = vec![];
    for (x, y, px) in img.enumerate_pixels() {
        // let alpha = px[3];
        if px[0] != empty_color.0
            || px[1] != empty_color.1
            || px[2] != empty_color.2
        {
            xs.push(x);
            ys.push(y);
        }
    }
    if xs.is_empty() || ys.is_empty() {
        img.clone()
    } else {
        // Safe to unwrap as we already check
        // if the vecs are empty.
        let min_x = xs.iter().min().unwrap();
        let max_x = xs.iter().max().unwrap();
        let min_y = ys.iter().min().unwrap();
        let max_y = ys.iter().max().unwrap();
        crop(img, *min_x, *min_y, max_x - min_x, max_y - min_y)
            .to_image()
    }
}

/// Get the line style for a particular index.
fn get_style(i: usize) -> ElemStyle {
    let color = COLORS[i % COLORS.len()];
    let symbol = match i / COLORS.len() % 4 {
        0 => Symbol::Circle,
        1 => Symbol::Triangle,
        2 => Symbol::Rect,
        3 => Symbol::Diamond,
        _ => panic!("impossible"),
    };
    let style = match i / COLORS.len() / 4 % 3 {
        0 => LineStyleType::Solid,
        1 => LineStyleType::Dashed,
        2 => LineStyleType::Dotted,
        _ => panic!("impossible"),
    };
    ElemStyle {
        color,
        symbol,
        line: style,
        ..Default::default()
    }
}

/// A convenient way to group plots together.
pub struct Plots {
    dir: PathBuf,
    pub paths: Vec<PathBuf>,
}
impl Plots {
    /// Create a set of plots that will be saved in the
    /// specified directory.
    pub fn new<P: AsRef<Path>>(save_dir: P) -> Plots {
        let save_dir = save_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&save_dir).unwrap();
        Plots {
            dir: save_dir,
            paths: vec![],
        }
    }

    /// Insert a new plot with the provided filename.
    /// The plot will be created using the provided function.
    pub fn insert(
        &mut self,
        fname: &str,
        chart_fn: impl FnOnce() -> StaticChart,
    ) {
        let chart = chart_fn();
        let save_path = self.dir.join(Path::new(fname));
        chart.render((1200, 800), &save_path);
        self.paths.push(save_path.to_path_buf());
    }

    /// Add a path to an existing plot to this group.
    pub fn add<P: AsRef<Path>>(&mut self, path: P) {
        self.paths.push(path.as_ref().to_path_buf());
    }

    /// Add multiple paths to existing plots to this group.
    pub fn extend<P: AsRef<Path>>(&mut self, paths: &[P]) {
        for path in paths {
            self.paths.push(path.as_ref().to_path_buf());
        }
    }

    /// Copy the plots from another `Plots` instance.
    pub fn append(&mut self, plots: Plots) {
        self.paths.extend(plots.paths);
    }
}

/// Take an SVG string and convert it to PNG data,
/// scaled up 2x.
fn svg_to_png(svg_data: String) -> RgbaImage {
    let opt = resvg::usvg::Options::default();
    let mut fontdb = resvg::usvg::fontdb::Database::new();
    fontdb.load_system_fonts();
    let tree = resvg::usvg::Tree::from_data(
        svg_data.as_bytes(),
        &opt,
        &fontdb,
    )
    .unwrap();

    let pixmap_size = tree.size().to_int_size();
    let mut pixmap = resvg::tiny_skia::Pixmap::new(
        pixmap_size.width() * 2,
        pixmap_size.height() * 2,
    )
    .unwrap();
    resvg::render(
        &tree,
        resvg::tiny_skia::Transform::from_scale(2., 2.),
        &mut pixmap.as_mut(),
    );
    let img_data = pixmap.encode_png().unwrap();
    image::load_from_memory_with_format(&img_data, ImageFormat::Png)
        .unwrap()
        .to_rgba8()
}

/// Convenience function for producing a boxplot.
pub fn boxplot<
    K: std::hash::Hash + std::fmt::Display + Clone,
    V,
    T: Into<NumericValue>,
>(
    groups: &HashMap<K, Vec<V>>,
    title: &str,
    key_fn: impl FnMut(&V) -> Option<T> + Clone,
    units: &str,
) -> StaticChart
where
    DataSource: From<Vec<Vec<T>>>,
{
    let data = extract_group_data(groups, key_fn);
    StaticChart::boxplot(data, units, true)
        .with_titles(title, "(#) = Sample Count")
}

/// Extract a pipeline group's data for plotting.
fn extract_group_data<
    K: std::hash::Hash + std::fmt::Display + Clone,
    V,
    T: Into<NumericValue>,
>(
    groups: &HashMap<K, Vec<V>>,
    key_fn: impl FnMut(&V) -> Option<T> + Clone,
) -> Vec<(String, Vec<T>)> {
    let mut to_boxplot: Vec<_> = groups
        .iter()
        .map(move |(k, vs)| {
            let valid: Vec<_> =
                vs.iter().filter_map(key_fn.clone()).collect();
            (format!("{k} ({})", valid.len()), valid)
        })
        .collect();
    to_boxplot.sort_by_cached_key(|(name, _)| name.clone());
    to_boxplot
}
