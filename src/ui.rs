use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap};
use serde_json::Value;

use crate::app::{App, DetailPaneMode, InputMode, TableOverlayKind};
use crate::model::{ResourceTab, RowData};

const BG: Color = Color::Rgb(9, 15, 25);
const PANEL: Color = Color::Rgb(16, 27, 44);
const ACCENT: Color = Color::Rgb(52, 211, 153);
const MUTED: Color = Color::Rgb(140, 156, 178);
const WARN: Color = Color::Rgb(251, 191, 36);
const ERROR: Color = Color::Rgb(248, 113, 113);
const PL_A: Color = Color::Rgb(17, 94, 89);
const PL_B: Color = Color::Rgb(30, 64, 175);
const PL_C: Color = Color::Rgb(55, 48, 163);
const PL_D: Color = Color::Rgb(82, 24, 124);
const PL_E: Color = Color::Rgb(13, 148, 136);

pub fn render(frame: &mut Frame, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(6),
            Constraint::Length(1),
        ])
        .split(frame.area());

    render_header(frame, root[0], app);
    render_body(frame, root[1], app);
    render_footer(frame, root[2], app);

    if app.show_help() {
        render_help_modal(frame, app);
    }
}

fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let left_line = build_left_header_line(app);
    if area.width < 42 {
        frame.render_widget(
            Paragraph::new(left_line).style(Style::default().bg(BG).fg(Color::White)),
            area,
        );
        return;
    }

    let right_line = build_right_header_line(app);
    let right_width = spans_width(&right_line.spans) as u16;
    if right_width == 0 || right_width >= area.width {
        frame.render_widget(
            Paragraph::new(left_line).style(Style::default().bg(BG).fg(Color::White)),
            area,
        );
        return;
    }
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(1), Constraint::Length(right_width)])
        .split(area);
    frame.render_widget(
        Paragraph::new(left_line).style(Style::default().bg(BG).fg(Color::White)),
        chunks[0],
    );

    frame.render_widget(
        Paragraph::new(right_line).style(Style::default().bg(BG)),
        chunks[1],
    );
}

fn build_left_header_line(app: &App) -> Line<'static> {
    let group = tab_group_label(app.active_tab());
    let group_icon = tab_group_icon(app.active_tab());
    let active_resource = if app.container_picker_active() {
        format!(
            "{} {}  {} {}",
            group_icon,
            compact_text(group, 10),
            "󰞷",
            "containers"
        )
    } else if app.table_overlay_active() {
        let (icon, label) = match app.table_overlay_kind() {
            Some(TableOverlayKind::PodLogs) => ("󰍩", "logs"),
            Some(TableOverlayKind::RelatedLogs) => ("󰌨", "logs"),
            Some(TableOverlayKind::Shell) => ("", "shell"),
            _ => (tab_icon(app.active_tab()), "output"),
        };
        format!(
            "{} {}  {} {}",
            group_icon,
            compact_text(group, 10),
            icon,
            compact_text(label, 14),
        )
    } else if app.active_tab() == ResourceTab::CustomResources {
        app.selected_custom_resource()
            .map(|crd| {
                format!(
                    "{} {}  {} {}",
                    group_icon,
                    compact_text(group, 10),
                    tab_icon(app.active_tab()),
                    compact_text(&crd.kind.to_ascii_lowercase(), 14),
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "{} {}  {} {}",
                    group_icon,
                    compact_text(group, 10),
                    tab_icon(app.active_tab()),
                    "crd"
                )
            })
    } else {
        let tab_title = app.active_tab().title().to_ascii_lowercase();
        format!(
            "{} {}  {} {}",
            group_icon,
            compact_text(group, 10),
            tab_icon(app.active_tab()),
            compact_text(&tab_title, 14),
        )
    };

    let mut spans = Vec::new();
    let argo_mode = matches!(
        app.active_tab(),
        ResourceTab::ArgoCdApps
            | ResourceTab::ArgoCdResources
            | ResourceTab::ArgoCdProjects
            | ResourceTab::ArgoCdRepos
            | ResourceTab::ArgoCdClusters
            | ResourceTab::ArgoCdAccounts
            | ResourceTab::ArgoCdCerts
            | ResourceTab::ArgoCdGpgKeys
    );
    push_powerline_segment(&mut spans, " ORCA ", Color::Black, ACCENT, PL_A);
    push_powerline_segment(
        &mut spans,
        format!(" 󰀄 {} ", compact_text(app.user(), 14)),
        Color::White,
        PL_A,
        PL_B,
    );
    if argo_mode {
        let server_value = compact_text(app.argocd_server(), 24);
        push_powerline_segment(
            &mut spans,
            format!(" 󰡨 {} ", server_value),
            Color::White,
            PL_B,
            PL_C,
        );
        push_powerline_segment(
            &mut spans,
            format!(
                " 󰙲 {} ",
                compact_text(app.argocd_selected_app().unwrap_or("-"), 16)
            ),
            Color::White,
            PL_C,
            PL_D,
        );
        push_powerline_segment(
            &mut spans,
            format!(
                " 󰈲 {} ",
                compact_text(
                    if app.filter().is_empty() {
                        "-"
                    } else {
                        app.filter()
                    },
                    14,
                )
            ),
            Color::White,
            PL_D,
            Color::Rgb(88, 28, 135),
        );
    } else {
        let cluster_value = compact_text(&display_cluster_endpoint(app.cluster()), 26);
        push_powerline_segment(
            &mut spans,
            format!(" 󰠳 {} ", cluster_value),
            Color::White,
            PL_B,
            PL_C,
        );
        push_powerline_segment(
            &mut spans,
            format!(" 󱃾 {} ", compact_text(app.context(), 14)),
            Color::White,
            PL_C,
            PL_D,
        );
        push_powerline_segment(
            &mut spans,
            format!(" 󰉖 {} ", compact_text(&app.namespace_scope().label(), 12)),
            Color::White,
            PL_D,
            Color::Rgb(88, 28, 135),
        );
        push_powerline_segment(
            &mut spans,
            format!(
                " 󰈲 {} ",
                compact_text(
                    if app.filter().is_empty() {
                        "-"
                    } else {
                        app.filter()
                    },
                    14,
                )
            ),
            Color::White,
            Color::Rgb(88, 28, 135),
            Color::Rgb(88, 28, 135),
        );
    }
    if let Some(port_forward) = app.port_forward_badge() {
        push_powerline_segment(
            &mut spans,
            format!(" {} ", active_resource),
            Color::White,
            Color::Rgb(88, 28, 135),
            PL_E,
        );
        push_powerline_segment(
            &mut spans,
            format!(" {} ", compact_text(&port_forward, 18)),
            Color::White,
            PL_E,
            BG,
        );
    } else {
        push_powerline_segment(
            &mut spans,
            format!(" {} ", active_resource),
            Color::White,
            Color::Rgb(88, 28, 135),
            BG,
        );
    }

    Line::from(spans)
}

fn build_right_header_line(app: &App) -> Line<'static> {
    let mut spans = Vec::new();
    let mut next_bg = BG;
    for slot in app.visible_view_slots() {
        let active = slot == app.active_view_slot();
        let initialized = app.view_slot_initialized(slot);
        let bg = if active {
            Color::Rgb(59, 130, 246)
        } else if initialized {
            Color::Rgb(67, 56, 202)
        } else {
            Color::Rgb(30, 41, 59)
        };
        let fg = if active { Color::Black } else { Color::White };
        push_powerline_segment_rtl(&mut spans, view_slot_label(slot, active), fg, bg, next_bg);
        next_bg = bg;
    }
    if !spans.is_empty() {
        spans.push(Span::styled(" ", Style::default().bg(next_bg)));
    }
    Line::from(spans)
}

fn render_body(frame: &mut Frame, area: Rect, app: &mut App) {
    app.set_table_page_size(table_rows_visible(area));
    let (table_width, table_height) = table_viewport(area);
    app.set_table_viewport(table_width, table_height);
    let (detail_width, detail_height) = detail_viewport(area);
    app.set_detail_viewport(detail_width, detail_height);

    if app.detail_mode() == DetailPaneMode::Details
        && !app.table_overlay_active()
        && !app.table_overview_active()
    {
        render_detail(frame, area, app, true);
    } else {
        render_table(frame, area, app, true);
    }
}

fn render_table(frame: &mut Frame, area: Rect, app: &App, focused: bool) {
    if app.container_picker_active() {
        render_container_picker(frame, area, app, focused);
        return;
    }

    if app.table_overlay_active() {
        let title = app
            .table_overlay_title()
            .map(str::to_string)
            .unwrap_or_else(|| "Output".to_string());
        let text = app.table_overlay_text().unwrap_or("");
        let paragraph = Paragraph::new(Text::from(text.to_string()))
            .wrap(Wrap { trim: false })
            .scroll((app.table_scroll(), 0))
            .block(
                Block::default()
                    .title(title)
                    .borders(Borders::ALL)
                    .border_style(if focused {
                        Style::default().fg(ACCENT)
                    } else {
                        Style::default().fg(MUTED)
                    })
                    .style(Style::default().bg(PANEL)),
            )
            .style(Style::default().fg(Color::White));
        frame.render_widget(paragraph, area);
        return;
    }

    if app.table_overview_active() {
        render_dashboard(frame, area, app, focused);
        return;
    }

    if let Some(error) = app.active_visible_error() {
        let panel = Paragraph::new(Text::from(error.to_string()))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(format!("{} Error", app.active_tab().title()))
                    .borders(Borders::ALL)
                    .border_style(if focused {
                        Style::default().fg(ERROR)
                    } else {
                        Style::default().fg(MUTED)
                    })
                    .style(Style::default().bg(PANEL)),
            )
            .style(Style::default().fg(ERROR));
        frame.render_widget(panel, area);
        return;
    }

    let active_tab = app.active_tab();
    let include_pf_column = matches!(active_tab, ResourceTab::Pods | ResourceTab::Services);
    let mut headers = app.active_headers();
    if include_pf_column {
        headers.push("PF".to_string());
    }
    let visible_rows = app.active_visible_rows();

    let header_row = Row::new(headers.iter().map(|header| {
        Cell::from(header.clone()).style(Style::default().add_modifier(Modifier::BOLD))
    }))
    .height(1)
    .style(Style::default().fg(ACCENT));

    let rows = visible_rows.iter().map(|row| {
        let mut columns = row.columns.clone();
        if include_pf_column {
            columns.push(app.port_forward_cell_for_row(active_tab, row));
        }

        Row::new(
            columns
                .into_iter()
                .map(|column| Cell::from(column).style(Style::default().fg(Color::White))),
        )
    });

    let constraints = column_constraints(headers.len().max(1));
    let title = if app.active_tab() == ResourceTab::Orca {
        format!("Dashboard ({})", visible_rows.len())
    } else {
        format!("{} ({})", app.active_tab().title(), visible_rows.len())
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(if focused {
            Style::default().fg(ACCENT)
        } else {
            Style::default().fg(MUTED)
        })
        .style(Style::default().bg(PANEL));

    let table = Table::new(rows, constraints)
        .header(header_row)
        .block(block)
        .column_spacing(1)
        .row_highlight_style(
            Style::default()
                .bg(Color::Rgb(24, 36, 58))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("󰜴 ");

    let mut state = TableState::default();
    state.select(app.active_selected_index());
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_container_picker(frame: &mut Frame, area: Rect, app: &App, focused: bool) {
    let title = app
        .container_picker_title()
        .unwrap_or_else(|| "Containers".to_string());
    let pod_name = app
        .container_picker_pod_name()
        .unwrap_or_else(|| "-".to_string());
    let headers = app.container_picker_headers();
    let items = app.container_picker_items();
    let header = Row::new(headers.iter().map(|header| {
        Cell::from(header.clone()).style(Style::default().add_modifier(Modifier::BOLD))
    }))
    .height(1)
    .style(Style::default().fg(ACCENT));
    let rows = items.iter().map(|item| {
        Row::new(vec![
            Cell::from(item.idx.to_string()).style(Style::default().fg(Color::White)),
            Cell::from(compact_text(&pod_name, 26)).style(Style::default().fg(Color::White)),
            Cell::from(item.name.clone()).style(Style::default().fg(Color::White)),
            Cell::from(compact_text(&item.image, 28)).style(Style::default().fg(Color::White)),
            Cell::from(item.ready.clone()).style(Style::default().fg(Color::White)),
            Cell::from(compact_text(&item.state, 16)).style(Style::default().fg(Color::White)),
            Cell::from(item.restarts.clone()).style(Style::default().fg(Color::White)),
            Cell::from(item.age.clone()).style(Style::default().fg(Color::White)),
            Cell::from(item.pf.clone()).style(Style::default().fg(Color::White)),
        ])
    });

    let block = Block::default()
        .title(format!("{title} ({})", items.len()))
        .borders(Borders::ALL)
        .border_style(if focused {
            Style::default().fg(ACCENT)
        } else {
            Style::default().fg(MUTED)
        })
        .style(Style::default().bg(PANEL));

    let table = Table::new(
        rows,
        vec![
            Constraint::Length(4),
            Constraint::Length(27),
            Constraint::Length(22),
            Constraint::Length(30),
            Constraint::Length(7),
            Constraint::Length(14),
            Constraint::Length(9),
            Constraint::Length(6),
            Constraint::Length(11),
        ],
    )
    .header(header)
    .block(block)
    .column_spacing(1)
    .row_highlight_style(
        Style::default()
            .bg(Color::Rgb(24, 36, 58))
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("󰜴 ");

    let mut state = TableState::default();
    state.select(app.container_picker_selected_index());
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_detail(frame: &mut Frame, area: Rect, app: &App, focused: bool) {
    let title = app.detail_title();
    let detail = app.detail_text();
    let text = if app.detail_overlay_active() {
        Text::from(detail)
    } else {
        highlight_structured_text(&detail)
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(if focused {
            Style::default().fg(ACCENT)
        } else {
            Style::default().fg(MUTED)
        })
        .style(Style::default().bg(PANEL));
    let paragraph = Paragraph::new(text)
        .block(block)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: false })
        .scroll((app.detail_scroll(), 0));

    frame.render_widget(paragraph, area);
}

fn render_dashboard(frame: &mut Frame, area: Rect, app: &App, focused: bool) {
    let model = build_dashboard_model(app);
    let block = Block::default()
        .title(model.title)
        .borders(Borders::ALL)
        .border_style(if focused {
            Style::default().fg(ACCENT)
        } else {
            Style::default().fg(MUTED)
        })
        .style(Style::default().bg(PANEL));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width < 20 || inner.height < 4 {
        return;
    }

    let gauge_count = model
        .bars
        .len()
        .min(inner.height.saturating_sub(1) as usize);
    let mut constraints = vec![Constraint::Length(1)];
    for _ in 0..gauge_count {
        constraints.push(Constraint::Length(1));
    }
    constraints.push(Constraint::Min(0));

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    let header = Paragraph::new(compact_text(
        &model.header,
        chunks[0].width.saturating_sub(1).max(1) as usize,
    ))
    .style(Style::default().fg(Color::Rgb(147, 197, 253)));
    frame.render_widget(header, chunks[0]);

    for (index, bar) in model.bars.iter().take(gauge_count).enumerate() {
        render_metric_gauge(frame, chunks[1 + index], bar);
    }
}

struct DashboardModel {
    title: String,
    header: String,
    bars: Vec<DashboardBar>,
}

struct DashboardBar {
    icon: &'static str,
    label: String,
    value: String,
    percent: u64,
    color: Color,
}

fn build_dashboard_model(app: &App) -> DashboardModel {
    let tab = app.active_tab();
    let rows = app.active_visible_rows();
    let selected = app.active_selected_row();
    let scores = rows
        .iter()
        .map(|row| row_health_score(tab, row))
        .collect::<Vec<_>>();

    let readiness_percent = if scores.is_empty() {
        0
    } else {
        scores
            .iter()
            .sum::<u64>()
            .saturating_div(scores.len() as u64)
    };
    let selected_percent = selected
        .map(|row| row_health_score(tab, row))
        .unwrap_or(readiness_percent);

    let (healthy, warning, risky) =
        scores
            .iter()
            .fold((0u64, 0u64, 0u64), |(ok, warn, risk), score| {
                if *score >= 80 {
                    (ok.saturating_add(1), warn, risk)
                } else if *score >= 50 {
                    (ok, warn.saturating_add(1), risk)
                } else {
                    (ok, warn, risk.saturating_add(1))
                }
            });
    let stability_percent = if scores.is_empty() {
        0
    } else {
        let weighted_risk = risky
            .saturating_mul(100)
            .saturating_add(warning.saturating_mul(50));
        100u64.saturating_sub(weighted_risk.saturating_div(scores.len() as u64))
    };

    let metrics = app.overview_metrics();
    let cpu_percent = metrics.cpu_percent.unwrap_or(0).min(100);
    let memory_percent = metrics.memory_percent.unwrap_or(0).min(100);

    let cpu_value = if metrics.cpu_capacity_millicores > 0 {
        format!(
            "{}/{}",
            format_cpu_millicores(metrics.cpu_usage_millicores),
            format_cpu_millicores(metrics.cpu_capacity_millicores)
        )
    } else if metrics.cpu_usage_millicores > 0 {
        format_cpu_millicores(metrics.cpu_usage_millicores)
    } else {
        "n/a".to_string()
    };
    let memory_value = if metrics.memory_capacity_bytes > 0 {
        format!(
            "{}/{}",
            format_bytes_compact(metrics.memory_usage_bytes),
            format_bytes_compact(metrics.memory_capacity_bytes)
        )
    } else if metrics.memory_usage_bytes > 0 {
        format_bytes_compact(metrics.memory_usage_bytes)
    } else {
        "n/a".to_string()
    };

    let selected_name = selected
        .map(|row| compact_text(&row.name, 16))
        .unwrap_or_else(|| "-".to_string());
    let selected_namespace = selected
        .and_then(|row| row.namespace.as_deref())
        .map(|value| compact_text(value, 12))
        .unwrap_or_else(|| "-".to_string());
    let selected_metric_detail = if let Some((cpu, memory)) = app.selected_resource_usage() {
        format!(
            "{} {}",
            format_cpu_millicores(cpu),
            format_bytes_compact(memory)
        )
    } else {
        selected
            .map(|row| compact_text(&selected_metric_line(tab, row), 22))
            .unwrap_or_else(|| "no metrics".to_string())
    };
    let selected_value = format!("{selected_namespace}/{selected_name} {selected_metric_detail}");

    let scope_label = app.namespace_scope().label();
    let filter_label = if app.filter().is_empty() {
        "-".to_string()
    } else {
        compact_text(app.filter(), 14)
    };
    let scope_value = format!(
        "ns:{} flt:{} pods:{} nodes:{}",
        compact_text(&scope_label, 10),
        filter_label,
        metrics.sampled_pods,
        metrics.sampled_nodes
    );

    let bars = vec![
        DashboardBar {
            icon: "󰓦",
            label: "Fleet Ready".to_string(),
            value: format!("ok:{healthy} warn:{warning} risk:{risky}"),
            percent: readiness_percent,
            color: score_color(readiness_percent),
        },
        DashboardBar {
            icon: "󰖌",
            label: "Stability".to_string(),
            value: format!("selected:{selected_percent}"),
            percent: stability_percent,
            color: score_color(stability_percent),
        },
        DashboardBar {
            icon: "󰾆",
            label: "CPU".to_string(),
            value: cpu_value,
            percent: cpu_percent,
            color: Color::Rgb(56, 189, 248),
        },
        DashboardBar {
            icon: "󰍛",
            label: "RAM".to_string(),
            value: memory_value,
            percent: memory_percent,
            color: Color::Rgb(147, 197, 253),
        },
        DashboardBar {
            icon: "󰙨",
            label: "Selected".to_string(),
            value: selected_value,
            percent: selected_percent,
            color: score_color(selected_percent),
        },
        DashboardBar {
            icon: "󰉖",
            label: "Scope".to_string(),
            value: scope_value,
            percent: 100,
            color: Color::Rgb(96, 165, 250),
        },
    ];

    DashboardModel {
        title: format!("{} Overview", tab.title()),
        header: format!(
            "{} {} ({})",
            tab_icon(tab),
            tab.title().to_ascii_lowercase(),
            rows.len()
        ),
        bars,
    }
}

fn score_color(score: u64) -> Color {
    if score >= 80 {
        ACCENT
    } else if score >= 55 {
        WARN
    } else {
        ERROR
    }
}

fn render_metric_gauge(frame: &mut Frame, area: Rect, bar: &DashboardBar) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(area);
    let percent = bar.percent.min(100);
    let label = format!("{} {} {}", bar.icon, bar.label, bar.value);
    let text = Paragraph::new(compact_text(
        &label,
        split[0].width.saturating_sub(1).max(1) as usize,
    ))
    .style(Style::default().fg(Color::Rgb(94, 234, 212)));
    frame.render_widget(text, split[0]);

    let bar_width = split[1].width as usize;
    if bar_width == 0 {
        return;
    }

    let mut filled = bar_width.saturating_mul(percent as usize) / 100;
    if percent > 0 && filled == 0 {
        filled = 1;
    }
    if percent >= 100 {
        filled = bar_width;
    }
    let meter_text = format!("{percent:>3}%");
    let meter_chars = meter_text.chars().collect::<Vec<_>>();
    let text_start = bar_width.saturating_sub(meter_chars.len()) / 2;
    let text_end = text_start.saturating_add(meter_chars.len());

    let mut spans = Vec::with_capacity(bar_width);
    for idx in 0..bar_width {
        let is_filled = idx < filled;
        let bg = if is_filled {
            bar.color
        } else {
            Color::Rgb(30, 41, 59)
        };
        let is_text_cell = idx >= text_start && idx < text_end;
        let ch = if is_text_cell {
            meter_chars[idx - text_start]
        } else {
            ' '
        };
        let fg = if is_filled {
            Color::Rgb(9, 15, 25)
        } else {
            Color::Rgb(148, 163, 184)
        };
        let mut style = Style::default().fg(fg).bg(bg);
        if is_text_cell {
            style = style.add_modifier(Modifier::BOLD);
        }
        spans.push(Span::styled(ch.to_string(), style));
    }
    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(PANEL)),
        split[1],
    );
}

fn format_cpu_millicores(value: u64) -> String {
    if value >= 1000 {
        let whole = value / 1000;
        let decimal = ((value % 1000) + 50) / 100;
        if decimal == 0 {
            format!("{whole}c")
        } else {
            format!("{whole}.{decimal}c")
        }
    } else {
        format!("{value}m")
    }
}

fn row_health_score(tab: ResourceTab, row: &RowData) -> u64 {
    match tab {
        ResourceTab::Orca => row
            .columns
            .get(3)
            .map(|value| value.to_ascii_lowercase())
            .map(|state| {
                if state.contains("ok") || state.contains("ready") || state.contains("online") {
                    100
                } else if state.contains("warn") || state.contains("mapped") {
                    60
                } else {
                    35
                }
            })
            .unwrap_or(70),
        ResourceTab::ArgoCdApps => {
            let sync = row
                .columns
                .get(3)
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default();
            let health = row
                .columns
                .get(4)
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default();
            if health.contains("healthy") && sync.contains("synced") {
                100
            } else if health.contains("progress") || sync.contains("outofsync") {
                60
            } else {
                35
            }
        }
        ResourceTab::ArgoCdResources => {
            let sync = row
                .columns
                .get(3)
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default();
            let health = row
                .columns
                .get(4)
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default();
            if health.contains("healthy") && sync.contains("synced") {
                100
            } else if health.contains("degraded")
                || health.contains("missing")
                || sync.contains("outofsync")
            {
                35
            } else {
                60
            }
        }
        ResourceTab::ArgoCdProjects => {
            let destinations = row
                .columns
                .get(2)
                .and_then(|value| parse_u64(value))
                .unwrap_or(0);
            let repos = row
                .columns
                .get(3)
                .and_then(|value| parse_u64(value))
                .unwrap_or(0);
            if destinations > 0 && repos > 0 {
                95
            } else if destinations > 0 || repos > 0 {
                70
            } else {
                45
            }
        }
        ResourceTab::ArgoCdRepos => {
            let insecure = row
                .columns
                .get(4)
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default();
            if insecure == "yes" { 65 } else { 90 }
        }
        ResourceTab::ArgoCdClusters => row
            .columns
            .get(2)
            .map(|value| value.to_ascii_lowercase())
            .map(|status| {
                if status.contains("successful") || status.contains("healthy") {
                    100
                } else if status.contains("unknown") {
                    50
                } else {
                    35
                }
            })
            .unwrap_or(50),
        ResourceTab::ArgoCdAccounts => row
            .columns
            .get(1)
            .map(|value| value.to_ascii_lowercase())
            .map(|enabled| {
                if enabled == "yes" || enabled == "true" {
                    90
                } else {
                    40
                }
            })
            .unwrap_or(50),
        ResourceTab::ArgoCdCerts | ResourceTab::ArgoCdGpgKeys => 85,
        ResourceTab::Pods => {
            let ready = row
                .columns
                .get(2)
                .and_then(|value| parse_ratio_percent(value))
                .unwrap_or(0);
            let status = row
                .columns
                .get(3)
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default();
            let restarts = row
                .columns
                .get(4)
                .and_then(|value| parse_u64(value))
                .unwrap_or(0);
            let mut score = ready;
            if status.contains("running") || status.contains("completed") {
                score = score.max(80);
            } else if status.contains("pending") || status.contains("init") {
                score = score.clamp(45, 75);
            } else if status.contains("failed")
                || status.contains("error")
                || status.contains("crash")
                || status.contains("backoff")
            {
                score = score.min(30);
            }
            score.saturating_sub(restarts.saturating_mul(5).min(35))
        }
        ResourceTab::Deployments
        | ResourceTab::DaemonSets
        | ResourceTab::ReplicaSets
        | ResourceTab::ReplicationControllers
        | ResourceTab::StatefulSets => row
            .columns
            .get(2)
            .and_then(|value| parse_ratio_percent(value))
            .unwrap_or(0),
        ResourceTab::CronJobs => {
            let suspended = row
                .columns
                .get(3)
                .map(|value| value.eq_ignore_ascii_case("yes"))
                .unwrap_or(false);
            let active = row
                .columns
                .get(4)
                .and_then(|value| parse_u64(value))
                .unwrap_or(0);
            if suspended {
                45
            } else if active > 0 {
                90
            } else {
                70
            }
        }
        ResourceTab::Jobs => {
            let completions = row
                .columns
                .get(2)
                .and_then(|value| parse_ratio_percent(value))
                .unwrap_or(0);
            let failed = row
                .columns
                .get(4)
                .and_then(|value| parse_u64(value))
                .unwrap_or(0);
            if failed > 0 {
                completions.min(45)
            } else {
                completions.max(60)
            }
        }
        ResourceTab::Services => row
            .columns
            .get(2)
            .map(|value| value.to_ascii_lowercase())
            .map(|service_type| {
                if service_type.contains("loadbalancer") {
                    95
                } else if service_type.contains("nodeport") {
                    85
                } else {
                    70
                }
            })
            .unwrap_or(65),
        ResourceTab::Ingresses | ResourceTab::IngressClasses => row
            .columns
            .get(4)
            .or_else(|| row.columns.get(2))
            .map(|value| value.to_ascii_lowercase())
            .map(|value| {
                if value == "-" || value.is_empty() {
                    60
                } else {
                    85
                }
            })
            .unwrap_or(70),
        ResourceTab::PersistentVolumeClaims => row
            .columns
            .get(2)
            .map(|value| value.to_ascii_lowercase())
            .map(|status| {
                if status.contains("bound") {
                    95
                } else if status.contains("pending") {
                    50
                } else {
                    30
                }
            })
            .unwrap_or(50),
        ResourceTab::PersistentVolumes => row
            .columns
            .get(4)
            .map(|value| value.to_ascii_lowercase())
            .map(|status| {
                if status.contains("bound") || status.contains("available") {
                    85
                } else if status.contains("released") {
                    55
                } else {
                    35
                }
            })
            .unwrap_or(50),
        ResourceTab::Nodes => row
            .columns
            .get(1)
            .map(|value| value.to_ascii_lowercase())
            .map(|status| {
                if status.contains("ready") && !status.contains("notready") {
                    100
                } else if status.contains("unknown") {
                    45
                } else {
                    25
                }
            })
            .unwrap_or(0),
        ResourceTab::Events => row
            .columns
            .get(4)
            .map(|value| value.to_ascii_lowercase())
            .map(|event_type| {
                if event_type.contains("normal") {
                    85
                } else if event_type.contains("warning") {
                    35
                } else {
                    55
                }
            })
            .unwrap_or(50),
        ResourceTab::Namespaces => row
            .columns
            .get(1)
            .map(|value| value.to_ascii_lowercase())
            .map(|status| if status == "active" { 100 } else { 45 })
            .unwrap_or(50),
        ResourceTab::ConfigMaps
        | ResourceTab::Secrets
        | ResourceTab::StorageClasses
        | ResourceTab::ServiceAccounts
        | ResourceTab::Roles
        | ResourceTab::RoleBindings
        | ResourceTab::ClusterRoles
        | ResourceTab::ClusterRoleBindings
        | ResourceTab::NetworkPolicies => {
            let score = row
                .columns
                .iter()
                .find_map(|value| parse_ratio_percent(value))
                .unwrap_or(75);
            score.clamp(55, 95)
        }
        ResourceTab::CustomResources => {
            let labels = row
                .columns
                .get(2)
                .and_then(|value| parse_u64(value))
                .unwrap_or(0);
            if labels > 0 { 80 } else { 65 }
        }
    }
}

fn selected_metric_line(tab: ResourceTab, row: &RowData) -> String {
    match tab {
        ResourceTab::Orca => format!(
            "domain:{} count:{} state:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::ArgoCdApps => format!(
            "project:{} ns:{} sync:{} health:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::ArgoCdResources => format!(
            "kind:{} sync:{} health:{}",
            row.columns.get(0).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::ArgoCdProjects => format!(
            "ns:{} dest:{} repos:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::ArgoCdRepos => format!(
            "type:{} project:{} oci:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(5).map_or("-", String::as_str)
        ),
        ResourceTab::ArgoCdClusters => format!(
            "status:{} version:{} apps:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::ArgoCdAccounts => format!(
            "enabled:{} caps:{}",
            row.columns.get(1).map_or("-", String::as_str),
            compact_text(row.columns.get(2).map_or("-", String::as_str), 18)
        ),
        ResourceTab::ArgoCdCerts => format!(
            "type:{} sub:{} fp:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str),
            compact_text(row.columns.get(3).map_or("-", String::as_str), 14)
        ),
        ResourceTab::ArgoCdGpgKeys => format!(
            "fingerprint:{} uids:{}",
            compact_text(row.columns.get(1).map_or("-", String::as_str), 16),
            compact_text(row.columns.get(2).map_or("-", String::as_str), 16)
        ),
        ResourceTab::Pods => format!(
            "ready:{} status:{} restarts:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::Deployments => format!(
            "ready:{} updated:{} available:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::DaemonSets => format!(
            "ready:{} updated:{} available:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::ReplicaSets => format!(
            "ready:{} available:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::ReplicationControllers => format!(
            "ready:{} current:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::StatefulSets => format!(
            "ready:{} current:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::CronJobs => format!(
            "schedule:{} active:{} last:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str),
            row.columns.get(5).map_or("-", String::as_str)
        ),
        ResourceTab::Jobs => format!(
            "completions:{} active:{} failed:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str)
        ),
        ResourceTab::Ingresses => format!(
            "class:{} hosts:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::IngressClasses => format!(
            "controller:{} default:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str)
        ),
        ResourceTab::Services => format!(
            "type:{} ports:{}",
            row.columns.get(2).map_or("-", String::as_str),
            compact_text(row.columns.get(4).map_or("-", String::as_str), 20)
        ),
        ResourceTab::ConfigMaps => format!(
            "data:{} binary:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::PersistentVolumeClaims => format!(
            "status:{} cap:{} access:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(4).map_or("-", String::as_str),
            compact_text(row.columns.get(5).map_or("-", String::as_str), 14)
        ),
        ResourceTab::Secrets => format!(
            "type:{} keys:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::StorageClasses => format!(
            "prov:{} default:{}",
            compact_text(row.columns.get(1).map_or("-", String::as_str), 14),
            row.columns.get(5).map_or("-", String::as_str)
        ),
        ResourceTab::PersistentVolumes => format!(
            "status:{} class:{}",
            row.columns.get(4).map_or("-", String::as_str),
            row.columns.get(6).map_or("-", String::as_str)
        ),
        ResourceTab::ServiceAccounts => format!(
            "namespace:{} secrets:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str)
        ),
        ResourceTab::Roles => format!(
            "namespace:{} rules:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str)
        ),
        ResourceTab::RoleBindings => format!(
            "role:{} subjects:{}",
            compact_text(row.columns.get(2).map_or("-", String::as_str), 16),
            row.columns.get(3).map_or("-", String::as_str)
        ),
        ResourceTab::ClusterRoles => format!(
            "rules:{} labels:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str)
        ),
        ResourceTab::ClusterRoleBindings => format!(
            "role:{} subjects:{}",
            compact_text(row.columns.get(1).map_or("-", String::as_str), 16),
            row.columns.get(2).map_or("-", String::as_str)
        ),
        ResourceTab::NetworkPolicies => format!(
            "selector:{} types:{}",
            row.columns.get(2).map_or("-", String::as_str),
            compact_text(row.columns.get(3).map_or("-", String::as_str), 14)
        ),
        ResourceTab::Nodes => format!(
            "state:{} role:{}",
            row.columns.get(1).map_or("-", String::as_str),
            compact_text(row.columns.get(2).map_or("-", String::as_str), 20)
        ),
        ResourceTab::Events => format!(
            "type:{} reason:{}",
            row.columns.get(4).map_or("-", String::as_str),
            compact_text(row.columns.get(3).map_or("-", String::as_str), 20)
        ),
        ResourceTab::Namespaces => format!(
            "status:{} labels:{}",
            row.columns.get(1).map_or("-", String::as_str),
            row.columns.get(2).map_or("-", String::as_str)
        ),
        ResourceTab::CustomResources => format!(
            "labels:{} age:{}",
            row.columns.get(2).map_or("-", String::as_str),
            row.columns.get(3).map_or("-", String::as_str)
        ),
    }
}

fn parse_ratio_percent(value: &str) -> Option<u64> {
    let (left, right) = value.split_once('/')?;
    let numerator = parse_u64(left)?;
    let denominator = parse_u64(right)?;
    if denominator == 0 {
        return Some(0);
    }
    Some(numerator.saturating_mul(100).saturating_div(denominator))
}

fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

fn render_footer(frame: &mut Frame, area: Rect, app: &App) {
    if matches!(app.mode(), InputMode::Normal) {
        let status_text = app
            .pending_confirmation_prompt()
            .map(|pending| format!("{pending}? (y/n)"))
            .unwrap_or_else(|| app.status().to_string());

        let mut spans = Vec::new();
        let status_bg = if app.pending_confirmation_prompt().is_some() {
            WARN
        } else {
            PL_B
        };
        let status_fg = if app.pending_confirmation_prompt().is_some() {
            Color::Black
        } else {
            Color::White
        };
        let status_icon = footer_status_icon(&status_text);
        let mode_bg = if app.read_only() { WARN } else { PL_A };
        let mode_fg = if app.read_only() {
            Color::Black
        } else {
            Color::White
        };
        let mode_label = if app.read_only() {
            " 󰌾 ro "
        } else {
            " 󰘳 nrm "
        };
        push_powerline_segment(&mut spans, mode_label, mode_fg, mode_bg, status_bg);
        let status_width_hint = if app.pending_confirmation_prompt().is_some() {
            area.width.saturating_sub(10) as usize
        } else {
            area.width.saturating_sub(24).min(120) as usize
        };
        push_powerline_segment(
            &mut spans,
            format!(
                " {status_icon} {} ",
                compact_text(&status_text, status_width_hint.max(24))
            ),
            status_fg,
            status_bg,
            BG,
        );
        let right_spans = if app.pending_confirmation_prompt().is_some() {
            Vec::new()
        } else {
            build_footer_glance_spans(app)
        };
        if right_spans.is_empty() {
            frame.render_widget(
                Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
                area,
            );
            return;
        }

        let min_left = 28u16;
        let max_right = area.width.saturating_sub(min_left);
        let right_width = (spans_width(&right_spans) as u16).min(max_right);
        if right_width == 0 {
            frame.render_widget(
                Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
                area,
            );
            return;
        }

        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(1), Constraint::Length(right_width)])
            .split(area);
        frame.render_widget(
            Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
            chunks[0],
        );
        frame.render_widget(
            Paragraph::new(Line::from(right_spans))
                .style(Style::default().bg(BG))
                .alignment(Alignment::Right),
            chunks[1],
        );
        return;
    }

    let (label, prompt, prompt_bg, prompt_fg) = match app.mode() {
        InputMode::Filter => (" 󰈲 flt ", format!("/{}", app.input()), WARN, Color::Black),
        InputMode::Command => (" 󰘳 cmd ", format!(":{}", app.input()), ACCENT, Color::Black),
        InputMode::Jump => (
            " 󰚭 jmp ",
            format!(">{}", app.input()),
            Color::Rgb(125, 211, 252),
            Color::Black,
        ),
        InputMode::Normal => unreachable!(),
    };

    let mut spans = Vec::new();
    push_powerline_segment(&mut spans, label, prompt_fg, prompt_bg, PL_B);
    push_powerline_segment(&mut spans, format!(" {} ", prompt), Color::White, PL_B, BG);

    if app.has_completion_mode() {
        let completions = app.completion_candidates();
        if !completions.is_empty() {
            let selected = app
                .completion_index()
                .min(completions.len().saturating_sub(1));
            let mut start = selected.saturating_sub(2);
            if start >= completions.len() {
                start = completions.len().saturating_sub(1);
            }
            let available_width = area.width as usize;
            let mut used_width = spans_width(&spans);
            if used_width < available_width {
                spans.push(Span::raw(" "));
                used_width = used_width.saturating_add(1);
            }
            if start > 0 {
                spans.push(Span::styled("… ", Style::default().fg(MUTED)));
                used_width = used_width.saturating_add(2);
            }
            for (absolute_index, item) in completions.iter().enumerate().skip(start) {
                let chunk = format!("{item} ");
                let chunk_width = chunk.chars().count();
                if used_width.saturating_add(chunk_width + 1) > available_width {
                    if absolute_index < completions.len().saturating_sub(1)
                        && used_width < available_width
                    {
                        spans.push(Span::styled("…", Style::default().fg(MUTED)));
                    }
                    break;
                }
                let style = if absolute_index == selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Rgb(94, 234, 212))
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(MUTED)
                };
                spans.push(Span::styled(chunk, style));
                used_width = used_width.saturating_add(chunk_width);
            }
        }
    }

    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
        area,
    );
}

fn build_footer_glance_spans(app: &App) -> Vec<Span<'static>> {
    if matches!(
        app.active_tab(),
        ResourceTab::ArgoCdApps
            | ResourceTab::ArgoCdResources
            | ResourceTab::ArgoCdProjects
            | ResourceTab::ArgoCdRepos
            | ResourceTab::ArgoCdClusters
            | ResourceTab::ArgoCdAccounts
            | ResourceTab::ArgoCdCerts
            | ResourceTab::ArgoCdGpgKeys
    ) {
        let visible_count = app.active_visible_rows().len();
        let selected = app.active_selected_row();
        let (field_one, field_two, field_three) = match app.active_tab() {
            ResourceTab::ArgoCdApps => (
                selected
                    .and_then(|row| row.columns.get(3))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .and_then(|row| row.columns.get(4))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                app.argocd_selected_app()
                    .map(|value| compact_text(value, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ResourceTab::ArgoCdResources => (
                selected
                    .and_then(|row| row.columns.get(3))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .and_then(|row| row.columns.get(4))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .map(|row| compact_text(&row.name, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ResourceTab::ArgoCdProjects => (
                selected
                    .and_then(|row| row.columns.get(2))
                    .map(|value| format!("dst:{value}"))
                    .unwrap_or_else(|| "dst:-".to_string()),
                selected
                    .and_then(|row| row.columns.get(3))
                    .map(|value| format!("repo:{value}"))
                    .unwrap_or_else(|| "repo:-".to_string()),
                selected
                    .map(|row| compact_text(&row.name, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ResourceTab::ArgoCdRepos => (
                selected
                    .and_then(|row| row.columns.get(1))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .and_then(|row| row.columns.get(4))
                    .map(|value| format!("tls:{value}"))
                    .unwrap_or_else(|| "tls:-".to_string()),
                selected
                    .map(|row| compact_text(&row.name, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ResourceTab::ArgoCdClusters => (
                selected
                    .and_then(|row| row.columns.get(2))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .and_then(|row| row.columns.get(4))
                    .map(|value| format!("apps:{value}"))
                    .unwrap_or_else(|| "apps:-".to_string()),
                selected
                    .map(|row| compact_text(&row.name, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ResourceTab::ArgoCdAccounts => (
                selected
                    .and_then(|row| row.columns.get(1))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .and_then(|row| row.columns.get(2))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .map(|row| compact_text(&row.name, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ResourceTab::ArgoCdCerts | ResourceTab::ArgoCdGpgKeys => (
                selected
                    .and_then(|row| row.columns.get(1))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .and_then(|row| row.columns.get(2))
                    .map(|value| compact_text(value, 12))
                    .unwrap_or_else(|| "-".to_string()),
                selected
                    .map(|row| compact_text(&row.name, 16))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            _ => ("-".to_string(), "-".to_string(), "-".to_string()),
        };
        let mut spans = Vec::new();
        let mut next_bg = BG;
        let segments = vec![
            (
                format!(" 󰀶 {} ", visible_count),
                Color::Black,
                Color::Rgb(45, 212, 191),
            ),
            (
                format!(" 󱎘 {} ", field_one),
                Color::Black,
                Color::Rgb(99, 102, 241),
            ),
            (
                format!(" 󰖌 {} ", field_two),
                Color::Black,
                Color::Rgb(74, 222, 128),
            ),
            (
                format!(" 󰙲 {} ", field_three),
                Color::White,
                Color::Rgb(124, 58, 237),
            ),
        ];
        for (content, fg, bg) in segments {
            push_powerline_segment_rtl(&mut spans, content, fg, bg, next_bg);
            next_bg = bg;
        }
        return spans;
    }

    let metrics = app.overview_metrics();
    let alerts = app.alert_snapshot();
    let selected_usage = app
        .selected_resource_usage()
        .filter(|(cpu, memory)| *cpu > 0 || *memory > 0);
    let selected_percent = app
        .active_selected_row()
        .map(|row| row_health_score(app.active_tab(), row).min(100))
        .unwrap_or(0);
    let visible_count = app.active_visible_rows().len();
    let cpu_value = if let Some((cpu, _)) = selected_usage {
        format_cpu_millicores(cpu)
    } else if metrics.cpu_capacity_millicores > 0 {
        format!(
            "{}/{}",
            format_cpu_millicores(metrics.cpu_usage_millicores),
            format_cpu_millicores(metrics.cpu_capacity_millicores)
        )
    } else {
        format_cpu_millicores(metrics.cpu_usage_millicores)
    };
    let memory_value = if let Some((_, memory)) = selected_usage {
        format_bytes_compact(memory)
    } else if metrics.memory_capacity_bytes > 0 {
        format!(
            "{}/{}",
            format_bytes_compact(metrics.memory_usage_bytes),
            format_bytes_compact(metrics.memory_capacity_bytes)
        )
    } else {
        format_bytes_compact(metrics.memory_usage_bytes)
    };

    let mut spans = Vec::new();
    let mut next_bg = BG;
    let segments = vec![
        (
            format!(" {} {} ", tab_icon(app.active_tab()), visible_count),
            Color::Black,
            Color::Rgb(45, 212, 191),
        ),
        (
            format!(" 󰖌 {}% ", selected_percent),
            Color::Black,
            Color::Rgb(74, 222, 128),
        ),
        (
            format!(" 󰾆 {} ", compact_text(&cpu_value, 12)),
            Color::White,
            Color::Rgb(37, 99, 235),
        ),
        (
            format!(" 󰍛 {} ", compact_text(&memory_value, 13)),
            Color::White,
            Color::Rgb(59, 130, 246),
        ),
        (
            format!(" 󰀦 {} ", alerts.warning_events),
            Color::White,
            Color::Rgb(79, 70, 229),
        ),
        (
            format!(" 󱎘 {} ", alerts.crash_loop_pods),
            Color::White,
            Color::Rgb(99, 102, 241),
        ),
        (
            format!(" 󰒋 {} ", alerts.not_ready_nodes),
            Color::White,
            Color::Rgb(124, 58, 237),
        ),
    ];

    for (content, fg, bg) in segments {
        push_powerline_segment_rtl(&mut spans, content, fg, bg, next_bg);
        next_bg = bg;
    }

    spans
}

fn footer_status_icon(status_text: &str) -> &'static str {
    let status = status_text.to_ascii_lowercase();
    let has_failure = [
        "failed",
        "error",
        "timed out",
        "timeout",
        "unreachable",
        "refused",
        "forbidden",
        "denied",
    ]
    .iter()
    .any(|needle| status.contains(needle));
    if has_failure { "󰅚" } else { "󰄬" }
}

fn highlight_structured_text(input: &str) -> Text<'static> {
    let trimmed = input.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<Value>(trimmed).is_ok()
    {
        return highlight_json_text(trimmed);
    }
    highlight_yaml_text(input)
}

fn highlight_json_text(input: &str) -> Text<'static> {
    let pretty = serde_json::from_str::<Value>(input)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| input.to_string());
    let lines = pretty
        .lines()
        .map(highlight_json_line)
        .collect::<Vec<Line<'static>>>();
    Text::from(lines)
}

fn highlight_json_line(line: &str) -> Line<'static> {
    let chars = line.chars().collect::<Vec<_>>();
    let mut index = 0usize;
    let mut spans = Vec::new();

    while index < chars.len() {
        let ch = chars[index];
        if ch.is_ascii_whitespace() {
            spans.push(Span::raw(ch.to_string()));
            index += 1;
            continue;
        }

        if matches!(ch, '{' | '}' | '[' | ']' | ':' | ',') {
            spans.push(Span::styled(ch.to_string(), Style::default().fg(MUTED)));
            index += 1;
            continue;
        }

        if ch == '"' {
            let (token, next_index) = read_json_string(&chars, index);
            let mut look_ahead = next_index;
            while look_ahead < chars.len() && chars[look_ahead].is_ascii_whitespace() {
                look_ahead += 1;
            }
            let color = if look_ahead < chars.len() && chars[look_ahead] == ':' {
                Color::Rgb(103, 232, 249)
            } else {
                Color::Rgb(125, 211, 252)
            };
            spans.push(Span::styled(token, Style::default().fg(color)));
            index = next_index;
            continue;
        }

        if ch.is_ascii_digit() || ch == '-' {
            let start = index;
            while index < chars.len()
                && (chars[index].is_ascii_digit()
                    || matches!(chars[index], '-' | '+' | '.' | 'e' | 'E'))
            {
                index += 1;
            }
            spans.push(Span::styled(
                chars[start..index].iter().collect::<String>(),
                Style::default().fg(Color::Rgb(251, 146, 60)),
            ));
            continue;
        }

        if chars[index..].starts_with(&['t', 'r', 'u', 'e'])
            || chars[index..].starts_with(&['f', 'a', 'l', 's', 'e'])
            || chars[index..].starts_with(&['n', 'u', 'l', 'l'])
        {
            let start = index;
            while index < chars.len() && chars[index].is_ascii_alphabetic() {
                index += 1;
            }
            spans.push(Span::styled(
                chars[start..index].iter().collect::<String>(),
                Style::default().fg(WARN),
            ));
            continue;
        }

        spans.push(Span::styled(
            ch.to_string(),
            Style::default().fg(Color::White),
        ));
        index += 1;
    }

    Line::from(spans)
}

fn read_json_string(chars: &[char], start: usize) -> (String, usize) {
    let mut index = start;
    let mut escaped = false;
    let mut token = String::new();
    while index < chars.len() {
        let ch = chars[index];
        token.push(ch);
        if index > start {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return (token, index + 1);
            }
        }
        index += 1;
    }
    (token, chars.len())
}

fn highlight_yaml_text(input: &str) -> Text<'static> {
    let lines = input
        .lines()
        .map(highlight_yaml_line)
        .collect::<Vec<Line<'static>>>();
    Text::from(lines)
}

fn highlight_yaml_line(line: &str) -> Line<'static> {
    let indent_len = line
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b' ' || **byte == b'\t')
        .count();
    let indent = &line[..indent_len];
    let trimmed = &line[indent_len..];

    let mut spans = vec![Span::raw(indent.to_string())];
    if trimmed.is_empty() {
        return Line::from(spans);
    }

    if let Some(comment) = trimmed.strip_prefix('#') {
        spans.push(Span::styled(
            format!("#{comment}"),
            Style::default().fg(MUTED),
        ));
        return Line::from(spans);
    }

    if let Some(rest) = trimmed.strip_prefix("- ") {
        spans.push(Span::styled("- ", Style::default().fg(ACCENT)));
        spans.extend(highlight_yaml_content(rest));
        return Line::from(spans);
    }

    spans.extend(highlight_yaml_content(trimmed));
    Line::from(spans)
}

fn highlight_yaml_content(content: &str) -> Vec<Span<'static>> {
    if let Some((key, value)) = split_yaml_key_value(content) {
        let mut spans = vec![
            Span::styled(
                key.to_string(),
                Style::default().fg(Color::Rgb(103, 232, 249)),
            ),
            Span::styled(":", Style::default().fg(MUTED)),
        ];

        if value.trim().is_empty() {
            return spans;
        }

        spans.push(Span::raw(" "));
        spans.push(Span::styled(
            value.trim_start().to_string(),
            Style::default().fg(yaml_value_color(value.trim())),
        ));
        spans
    } else {
        vec![Span::styled(
            content.to_string(),
            Style::default().fg(Color::White),
        )]
    }
}

fn split_yaml_key_value(content: &str) -> Option<(&str, &str)> {
    let (key, value) = content.split_once(':')?;
    let key = key.trim_end();
    if key.is_empty() || key.contains(' ') {
        return None;
    }
    Some((key, value))
}

fn yaml_value_color(value: &str) -> Color {
    if value.starts_with('"') || value.starts_with('\'') {
        Color::Rgb(125, 211, 252)
    } else if matches!(value, "true" | "false" | "null" | "~") {
        WARN
    } else if value.parse::<f64>().is_ok() {
        Color::Rgb(251, 146, 60)
    } else if value.starts_with('{') || value.starts_with('[') {
        MUTED
    } else {
        Color::Rgb(147, 197, 253)
    }
}

fn push_powerline_segment(
    spans: &mut Vec<Span<'static>>,
    content: impl Into<String>,
    fg: Color,
    bg: Color,
    next_bg: Color,
) {
    spans.push(Span::styled(
        content.into(),
        Style::default().fg(fg).bg(bg).add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::styled("", Style::default().fg(bg).bg(next_bg)));
}

fn push_powerline_segment_rtl(
    spans: &mut Vec<Span<'static>>,
    content: impl Into<String>,
    fg: Color,
    bg: Color,
    next_bg: Color,
) {
    spans.push(Span::styled("", Style::default().fg(bg).bg(next_bg)));
    spans.push(Span::styled(
        content.into(),
        Style::default().fg(fg).bg(bg).add_modifier(Modifier::BOLD),
    ));
}

fn view_slot_label(slot: usize, active: bool) -> String {
    if active {
        format!("◉{slot}")
    } else {
        slot.to_string()
    }
}

fn spans_width(spans: &[Span<'_>]) -> usize {
    spans.iter().map(|span| span.content.chars().count()).sum()
}

fn render_help_modal(frame: &mut Frame, app: &App) {
    let area = centered_rect(78, 72, frame.area());
    frame.render_widget(Clear, area);

    let mut lines = vec![
        Line::from(format!(
            "orca help  mode:{}  scope:{}  tab:{}",
            help_mode_label(app.mode()),
            app.namespace_scope(),
            app.active_tab().title()
        )),
        Line::from(""),
    ];
    for line in contextual_help_lines(app) {
        lines.push(Line::from(line));
    }

    let modal = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title("Help")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ACCENT))
                .style(Style::default().bg(PANEL)),
        )
        .style(Style::default().fg(Color::White));

    frame.render_widget(modal, area);
}

fn contextual_help_lines(app: &App) -> Vec<String> {
    let mut lines = Vec::new();

    lines.push("Flow: Enter drill-down  Esc step-back  d details  o overview".to_string());
    lines.push(
        "Views: Ctrl+1..9 switch/create  Ctrl+Shift+1..9 mirror  Ctrl+Alt+0..9 delete".to_string(),
    );
    lines.push("Hotkeys: runtime bindings from orca.yaml are active in normal mode".to_string());
    lines.push(
        "Catalog: :ctx list/switch  :cluster list/switch  :usr list/switch  :ns list/scope"
            .to_string(),
    );
    lines.push("Config: :config view runtime aliases/plugins (auto-reload)".to_string());
    lines.push("Safety: :readonly on|off|toggle (blocks mutating actions)".to_string());
    lines.push(
        "Ops: :tools  :alerts  :argocd  :helm  :tf  :ansible  :docker  :rbac  :who-can  :oc  :kustomize  :git  :plugin"
            .to_string(),
    );
    lines.push("SRE: :pulses fleet snapshot  :xray selected-resource relations".to_string());
    lines.push("Input: : command  > jump  / filter  Tab autocomplete  Ctrl+u/d page".to_string());
    lines.push(String::new());

    if app.shell_overlay_active() {
        lines.push("Shell pane active".to_string());
        lines.push("Keys: Enter run  Esc close shell  arrows/home/end move cursor".to_string());
        lines.push("Edit: Backspace/Delete  Ctrl+a/e line bounds  Ctrl+u/k cut line".to_string());
        lines.push("Commands: :shell [container] [auto|/bin/bash|/bin/sh]".to_string());
        return lines;
    }

    if app.container_picker_active() {
        lines.push("Container picker active".to_string());
        lines.push("Keys: j/k select container  Enter or l open logs  Esc back to pod".to_string());
        lines.push("Commands: :shell <container> [auto|/bin/bash]  :exec <cmd>".to_string());
        return lines;
    }

    if app.table_overlay_active() {
        lines.push("Output pane active".to_string());
        lines.push("Keys: j/k or Ctrl+u/d scroll  gg/G top/bottom  Esc close output".to_string());
        lines.push(
            "Use Enter from table rows to drill deeper, then l/Shift+L for logs.".to_string(),
        );
        return lines;
    }

    if app.table_overview_active() {
        lines.push("Overview pane active".to_string());
        lines.push(
            "Keys: o toggle overview  Esc close overview  j/k keep selection in table".to_string(),
        );
        lines
            .push("Metrics follow selected row when available, fallback to aggregate.".to_string());
        return lines;
    }

    lines.push(format!(
        "Selected resource: {} (alias: {})",
        app.active_tab().title(),
        app.active_tab().short_token()
    ));
    lines.push(resource_tab_help(app.active_tab()));
    lines.push(resource_commands_help(app.active_tab()));
    if app.active_tab() == ResourceTab::Orca {
        lines.push(
            "Global ops: Enter drill-down  :orca  :k8s  :argocd  :tools  r refresh  ? close help  q quit"
                .to_string(),
        );
    } else if app.active_tab() == ResourceTab::ArgoCdResources {
        lines.push(
            "Global ops: e events  l logs  m manifest  s shell (Pod)  r refresh  ? close help  q quit"
                .to_string(),
        );
    } else {
        lines.push(
            "Global ops: e edit  p port-forward  r refresh  :pulses  :xray  ? close help  q quit"
                .to_string(),
        );
    }
    lines
}

fn resource_tab_help(tab: ResourceTab) -> String {
    match tab {
        ResourceTab::Orca => {
            "ORCA graph: Enter drills into k8s, argocd, and service nodes".to_string()
        }
        ResourceTab::ArgoCdApps => {
            "Argo CD flow: Enter opens selected app resources  e edit app manifest  d details"
                .to_string()
        }
        ResourceTab::ArgoCdResources => {
            "Argo CD resources: Enter full panel  e events  l logs  m live manifest  d raw details"
                .to_string()
        }
        ResourceTab::ArgoCdProjects => {
            "Argo CD projects: Enter/d opens project spec details".to_string()
        }
        ResourceTab::ArgoCdRepos => {
            "Argo CD repositories: Enter/d opens repository config details".to_string()
        }
        ResourceTab::ArgoCdClusters => {
            "Argo CD clusters: Enter/d opens cluster connection details".to_string()
        }
        ResourceTab::ArgoCdAccounts => {
            "Argo CD accounts: Enter/d opens account capabilities".to_string()
        }
        ResourceTab::ArgoCdCerts => {
            "Argo CD certs: Enter/d opens known hosts / cert records".to_string()
        }
        ResourceTab::ArgoCdGpgKeys => "Argo CD GPG: Enter/d opens signing key metadata".to_string(),
        ResourceTab::Pods => {
            "Pod flow: Enter containers  l container logs  Shift+L related logs  s shell"
                .to_string()
        }
        ResourceTab::Deployments
        | ResourceTab::StatefulSets
        | ResourceTab::DaemonSets
        | ResourceTab::ReplicaSets
        | ResourceTab::ReplicationControllers
        | ResourceTab::Jobs
        | ResourceTab::CronJobs => {
            "Workload flow: Enter pods  Shift+L workload logs  d details  Esc to parent".to_string()
        }
        ResourceTab::Services => {
            "Service flow: Enter related pods  Shift+L service logs  p port-forward".to_string()
        }
        ResourceTab::Namespaces => {
            "Namespace flow: Enter namespace to switch scope and open Pods".to_string()
        }
        ResourceTab::CustomResources => {
            "CRD flow: :crd <name|kind|plural> choose resource, Enter to navigate rows".to_string()
        }
        _ => "Resource flow: Enter if supported, d details for full object manifest.".to_string(),
    }
}

fn resource_commands_help(tab: ResourceTab) -> String {
    match tab {
        ResourceTab::Orca => {
            "Commands: :orca  :k8s [resource]  :argocd [app]  :tools  :alerts  :pulses"
                .to_string()
        }
        ResourceTab::ArgoCdApps | ResourceTab::ArgoCdResources => {
            "Commands: :argocd [app]  :argocd resources  Enter panel  :argocd sync|refresh|diff|history|rollback|delete [app]"
                .to_string()
        }
        ResourceTab::ArgoCdProjects
        | ResourceTab::ArgoCdRepos
        | ResourceTab::ArgoCdClusters
        | ResourceTab::ArgoCdAccounts
        | ResourceTab::ArgoCdCerts
        | ResourceTab::ArgoCdGpgKeys => {
            "Commands: :argocd projects|repos|clusters|accounts|certs|gpg  / filter".to_string()
        }
        ResourceTab::Pods => {
            "Commands: :logs  :shell [container]  :exec <cmd...>  :port-forward <L:R>".to_string()
        }
        ResourceTab::Deployments | ResourceTab::StatefulSets => {
            "Commands: :scale <replicas>  :restart  :edit  :delete".to_string()
        }
        ResourceTab::Services => "Commands: :port-forward <L:R>  :edit  :delete".to_string(),
        ResourceTab::Namespaces => {
            "Commands: :ns <name> set scope  :all-ns clear scope".to_string()
        }
        _ => "Commands: :edit  :delete (where supported)  :filter <expr>  :clear".to_string(),
    }
}

fn help_mode_label(mode: InputMode) -> &'static str {
    match mode {
        InputMode::Normal => "normal",
        InputMode::Filter => "filter",
        InputMode::Command => "command",
        InputMode::Jump => "jump",
    }
}

fn table_rows_visible(area: Rect) -> usize {
    area.height.saturating_sub(3).max(1) as usize
}

fn table_viewport(area: Rect) -> (u16, u16) {
    let width = area.width.saturating_sub(2).max(1);
    let height = area.height.saturating_sub(2).max(1);
    (width, height)
}

fn detail_viewport(area: Rect) -> (u16, u16) {
    let width = area.width.saturating_sub(2).max(1);
    let height = area.height.saturating_sub(2).max(1);
    (width, height)
}

fn compact_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    if max_chars <= 1 {
        return "…".to_string();
    }

    let mut out = value
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    out.push('…');
    out
}

fn display_cluster_endpoint(cluster: &str) -> String {
    cluster
        .trim()
        .trim_end_matches('/')
        .strip_prefix("https://")
        .or_else(|| cluster.trim().trim_end_matches('/').strip_prefix("http://"))
        .unwrap_or(cluster.trim().trim_end_matches('/'))
        .to_string()
}

fn tab_icon(tab: ResourceTab) -> &'static str {
    match tab {
        ResourceTab::Orca => "",
        ResourceTab::ArgoCdApps => "󰀶",
        ResourceTab::ArgoCdResources => "󰛀",
        ResourceTab::ArgoCdProjects => "󰠱",
        ResourceTab::ArgoCdRepos => "󰳏",
        ResourceTab::ArgoCdClusters => "󰠳",
        ResourceTab::ArgoCdAccounts => "󰀉",
        ResourceTab::ArgoCdCerts => "󰌆",
        ResourceTab::ArgoCdGpgKeys => "󰯄",
        ResourceTab::Pods => "󰋊",
        ResourceTab::CronJobs => "󰃰",
        ResourceTab::DaemonSets => "󰠱",
        ResourceTab::Deployments => "󰹑",
        ResourceTab::ReplicaSets => "󰹍",
        ResourceTab::ReplicationControllers => "󰐌",
        ResourceTab::StatefulSets => "󰛨",
        ResourceTab::Jobs => "󰁨",
        ResourceTab::Services => "󰒓",
        ResourceTab::Ingresses => "󰇚",
        ResourceTab::IngressClasses => "󰊠",
        ResourceTab::ConfigMaps => "󰈙",
        ResourceTab::PersistentVolumeClaims => "󱃞",
        ResourceTab::Secrets => "󰌋",
        ResourceTab::StorageClasses => "󰆼",
        ResourceTab::PersistentVolumes => "󱃔",
        ResourceTab::ServiceAccounts => "󰯃",
        ResourceTab::Roles => "󰒃",
        ResourceTab::RoleBindings => "󰑖",
        ResourceTab::ClusterRoles => "󰒄",
        ResourceTab::ClusterRoleBindings => "󰑗",
        ResourceTab::NetworkPolicies => "󰅙",
        ResourceTab::Nodes => "󰣇",
        ResourceTab::Events => "󱐋",
        ResourceTab::Namespaces => "󰉖",
        ResourceTab::CustomResources => "󰚜",
    }
}

fn tab_group_label(tab: ResourceTab) -> &'static str {
    match tab {
        ResourceTab::Orca => "orca",
        ResourceTab::ArgoCdApps
        | ResourceTab::ArgoCdResources
        | ResourceTab::ArgoCdProjects
        | ResourceTab::ArgoCdRepos
        | ResourceTab::ArgoCdClusters
        | ResourceTab::ArgoCdAccounts
        | ResourceTab::ArgoCdCerts
        | ResourceTab::ArgoCdGpgKeys => "argocd",
        ResourceTab::Pods
        | ResourceTab::CronJobs
        | ResourceTab::DaemonSets
        | ResourceTab::Deployments
        | ResourceTab::ReplicaSets
        | ResourceTab::ReplicationControllers
        | ResourceTab::StatefulSets
        | ResourceTab::Jobs => "workloads",
        ResourceTab::Services | ResourceTab::Ingresses | ResourceTab::IngressClasses => "service",
        ResourceTab::ConfigMaps
        | ResourceTab::PersistentVolumeClaims
        | ResourceTab::Secrets
        | ResourceTab::StorageClasses
        | ResourceTab::PersistentVolumes => "config",
        ResourceTab::ServiceAccounts
        | ResourceTab::Roles
        | ResourceTab::RoleBindings
        | ResourceTab::ClusterRoles
        | ResourceTab::ClusterRoleBindings
        | ResourceTab::NetworkPolicies
        | ResourceTab::Nodes
        | ResourceTab::Events
        | ResourceTab::Namespaces => "cluster",
        ResourceTab::CustomResources => "crd",
    }
}

fn tab_group_icon(tab: ResourceTab) -> &'static str {
    match tab_group_label(tab) {
        "orca" => "",
        "argocd" => "󰀶",
        "workloads" => "󰙨",
        "service" => "󰒓",
        "config" => "󰈙",
        "cluster" => "󰠳",
        "crd" => "󰚜",
        _ => "󰀄",
    }
}

fn format_bytes_compact(bytes: u64) -> String {
    const UNITS: [(&str, u64); 6] = [
        ("Ei", 1_152_921_504_606_846_976),
        ("Pi", 1_125_899_906_842_624),
        ("Ti", 1_099_511_627_776),
        ("Gi", 1_073_741_824),
        ("Mi", 1_048_576),
        ("Ki", 1_024),
    ];

    if bytes == 0 {
        return "0B".to_string();
    }

    for (suffix, unit) in UNITS {
        if bytes >= unit {
            let whole = bytes / unit;
            let decimal = ((bytes % unit) * 10) / unit;
            if decimal == 0 {
                return format!("{whole}{suffix}");
            }
            return format!("{whole}.{decimal}{suffix}");
        }
    }

    format!("{bytes}B")
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn column_constraints(columns: usize) -> Vec<Constraint> {
    if columns == 0 {
        return vec![Constraint::Percentage(100)];
    }

    let width = (100 / columns as u16).max(1);
    (0..columns)
        .map(|_| Constraint::Percentage(width))
        .collect()
}
