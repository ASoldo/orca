use crate::app::InputMode;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Quit,
    NextTab,
    PrevTab,
    Down,
    Up,
    PageDown,
    PageUp,
    Top,
    Bottom,
    ToggleHelp,
    ToggleFocus,
    EnterResource,
    ShowDetails,
    StartCommand,
    StartJump,
    StartFilter,
    Refresh,
    LoadPodLogs,
    LoadResourceLogs,
    OpenPodShell,
    EditResource,
    ShowManifest,
    StartPortForwardPrompt,
    ToggleOverview,
    ClearDetailOverlay,
    GPrefix,
    SubmitInput,
    CompleteInput,
    NextSuggestion,
    PrevSuggestion,
    CancelInput,
    Backspace,
    Delete,
    InputChar(char),
    ConfirmYes,
    ConfirmNo,
    SwitchView(u8),
    DeleteView(u8),
}

pub fn map_key(mode: InputMode, key: KeyEvent) -> Option<Action> {
    match mode {
        InputMode::Normal => map_normal_mode_key(key),
        InputMode::Command | InputMode::Filter | InputMode::Jump => map_input_mode_key(key),
    }
}

fn map_normal_mode_key(key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Char(c) if key.modifiers.is_empty() && c.is_ascii_digit() => {
            Some(Action::SwitchView(c.to_digit(10).unwrap_or(0) as u8))
        }
        KeyCode::Char(')') if key.modifiers.is_empty() => Some(Action::SwitchView(0)),
        KeyCode::Char(')') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(0))
        }
        KeyCode::Char(' ') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(0))
        }
        KeyCode::Char('j') if key.modifiers.is_empty() => Some(Action::Down),
        KeyCode::Down => Some(Action::Down),
        KeyCode::Char('k') if key.modifiers.is_empty() => Some(Action::Up),
        KeyCode::Up => Some(Action::Up),
        KeyCode::Left => Some(Action::PrevTab),
        KeyCode::Right => Some(Action::NextTab),
        KeyCode::Char('g') => Some(Action::GPrefix),
        KeyCode::Char('G') => Some(Action::Bottom),
        KeyCode::Home => Some(Action::Top),
        KeyCode::Char('?') => Some(Action::ToggleHelp),
        KeyCode::Char('r') | KeyCode::F(5) => Some(Action::Refresh),
        KeyCode::Char('/') => Some(Action::StartFilter),
        KeyCode::Char(':') => Some(Action::StartCommand),
        KeyCode::Char(';') if key.modifiers.contains(KeyModifiers::SHIFT) => {
            Some(Action::StartCommand)
        }
        KeyCode::Char('>') => Some(Action::StartJump),
        KeyCode::Char('.') if key.modifiers.contains(KeyModifiers::SHIFT) => {
            Some(Action::StartJump)
        }
        KeyCode::Char('l') => Some(Action::LoadPodLogs),
        KeyCode::Char('L') => Some(Action::LoadResourceLogs),
        KeyCode::Char('s') => Some(Action::OpenPodShell),
        KeyCode::Char('e') => Some(Action::EditResource),
        KeyCode::Char('m') if key.modifiers.is_empty() => Some(Action::ShowManifest),
        KeyCode::Char('p') => Some(Action::StartPortForwardPrompt),
        KeyCode::Char('o') => Some(Action::ToggleOverview),
        KeyCode::Char('d') if key.modifiers.is_empty() => Some(Action::ShowDetails),
        KeyCode::Char('y') | KeyCode::Char('Y') => Some(Action::ConfirmYes),
        KeyCode::Char('n') | KeyCode::Char('N') => Some(Action::ConfirmNo),
        KeyCode::Tab if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(9))
        }
        KeyCode::Tab => Some(Action::ToggleFocus),
        KeyCode::Enter => Some(Action::EnterResource),
        KeyCode::Char('m') | KeyCode::Char('j')
            if key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            Some(Action::EnterResource)
        }
        KeyCode::Esc if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(3))
        }
        KeyCode::Esc => Some(Action::ClearDetailOverlay),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::PageDown)
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => Some(Action::PageUp),
        KeyCode::Char(c)
            if key.modifiers.contains(KeyModifiers::CONTROL)
                && key.modifiers.contains(KeyModifiers::ALT) =>
        {
            map_digit_number(c).map(Action::DeleteView)
        }
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            map_ctrl_number(c).map(Action::SwitchView)
        }
        KeyCode::Null
            if key.modifiers.is_empty() || key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            Some(Action::SwitchView(0))
        }
        KeyCode::Backspace if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(8))
        }
        KeyCode::Backspace if key.modifiers.is_empty() => Some(Action::SwitchView(8)),
        _ => None,
    }
}

fn map_ctrl_number(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c.to_digit(10).unwrap_or(0) as u8),
        '!' => Some(1),
        '@' => Some(2),
        '#' => Some(3),
        '$' => Some(4),
        '%' => Some(5),
        '&' => Some(7),
        '*' => Some(8),
        '(' => Some(9),
        ')' => Some(0),
        ' ' => Some(0),
        'a' => Some(1),
        'b' => Some(2),
        '[' => Some(3),
        '\\' => Some(4),
        ']' => Some(5),
        '^' => Some(6),
        '_' => Some(7),
        'h' => Some(8),
        'i' => Some(9),
        _ => None,
    }
}

fn map_digit_number(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c.to_digit(10).unwrap_or(0) as u8),
        ')' => Some(0),
        _ => None,
    }
}

fn map_input_mode_key(key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Esc if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(3))
        }
        KeyCode::Esc => Some(Action::CancelInput),
        KeyCode::Char(')') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(0))
        }
        KeyCode::Enter => Some(Action::SubmitInput),
        KeyCode::Char('m') | KeyCode::Char('j')
            if key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            Some(Action::SubmitInput)
        }
        KeyCode::Tab if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(9))
        }
        KeyCode::Backspace if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(8))
        }
        KeyCode::Null
            if key.modifiers.is_empty() || key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            Some(Action::SwitchView(0))
        }
        KeyCode::Char(' ') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(0))
        }
        KeyCode::Tab => Some(Action::CompleteInput),
        KeyCode::Backspace => Some(Action::Backspace),
        KeyCode::Delete => Some(Action::Delete),
        KeyCode::Down => Some(Action::NextSuggestion),
        KeyCode::Up => Some(Action::PrevSuggestion),
        KeyCode::Char(c) if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT => {
            Some(Action::InputChar(c))
        }
        KeyCode::Char('w') if key.modifiers.contains(KeyModifiers::CONTROL) => Some(Action::Delete),
        KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::NextSuggestion)
        }
        KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::PrevSuggestion)
        }
        KeyCode::Char(c)
            if key.modifiers.contains(KeyModifiers::CONTROL)
                && key.modifiers.contains(KeyModifiers::ALT) =>
        {
            map_digit_number(c).map(Action::DeleteView)
        }
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            map_ctrl_number(c).map(Action::SwitchView)
        }
        _ => None,
    }
}

pub fn key_event_signature(key: KeyEvent) -> Option<String> {
    let key_name = match key.code {
        KeyCode::Char(' ') => "space".to_string(),
        KeyCode::Char('+') => "plus".to_string(),
        KeyCode::Char(c) => c.to_ascii_lowercase().to_string(),
        KeyCode::Enter => "enter".to_string(),
        KeyCode::Tab => "tab".to_string(),
        KeyCode::BackTab => "backtab".to_string(),
        KeyCode::Backspace => "backspace".to_string(),
        KeyCode::Delete => "delete".to_string(),
        KeyCode::Insert => "insert".to_string(),
        KeyCode::Esc => "esc".to_string(),
        KeyCode::Left => "left".to_string(),
        KeyCode::Right => "right".to_string(),
        KeyCode::Up => "up".to_string(),
        KeyCode::Down => "down".to_string(),
        KeyCode::Home => "home".to_string(),
        KeyCode::End => "end".to_string(),
        KeyCode::PageUp => "pageup".to_string(),
        KeyCode::PageDown => "pagedown".to_string(),
        KeyCode::F(n) => format!("f{n}"),
        _ => return None,
    };

    let mut parts = Vec::new();
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        parts.push("ctrl".to_string());
    }
    if key.modifiers.contains(KeyModifiers::ALT) {
        parts.push("alt".to_string());
    }
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        parts.push("shift".to_string());
    }
    parts.push(key_name);
    Some(parts.join("+"))
}

pub fn normalize_hotkey_spec(spec: &str) -> Option<String> {
    let mut ctrl = false;
    let mut alt = false;
    let mut shift = false;
    let mut key: Option<String> = None;

    for token in spec
        .split('+')
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty())
    {
        match token.as_str() {
            "ctrl" | "control" => ctrl = true,
            "alt" => alt = true,
            "shift" => shift = true,
            _ => {
                key = normalize_hotkey_key_token(&token);
            }
        }
    }

    let key = key?;
    let mut parts = Vec::new();
    if ctrl {
        parts.push("ctrl".to_string());
    }
    if alt {
        parts.push("alt".to_string());
    }
    if shift {
        parts.push("shift".to_string());
    }
    parts.push(key);
    Some(parts.join("+"))
}

fn normalize_hotkey_key_token(token: &str) -> Option<String> {
    match token {
        "esc" | "escape" => Some("esc".to_string()),
        "return" => Some("enter".to_string()),
        "pgup" => Some("pageup".to_string()),
        "pgdn" => Some("pagedown".to_string()),
        "del" => Some("delete".to_string()),
        "ins" => Some("insert".to_string()),
        "space" | "plus" | "tab" | "backtab" | "enter" | "backspace" | "delete" | "insert"
        | "left" | "right" | "up" | "down" | "home" | "end" | "pageup" | "pagedown" => {
            Some(token.to_string())
        }
        _ if token.len() == 1 => Some(token.to_string()),
        _ if token.starts_with('f') => {
            let number = token.trim_start_matches('f').parse::<u8>().ok()?;
            if (1..=24).contains(&number) {
                Some(format!("f{number}"))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, key_event_signature, map_key, normalize_hotkey_spec};
    use crate::app::InputMode;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    #[test]
    fn normal_mode_maps_quit() {
        let key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::Quit));
    }

    #[test]
    fn input_mode_maps_char() {
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        let action = map_key(InputMode::Command, key);
        assert_eq!(action, Some(Action::InputChar('a')));
    }

    #[test]
    fn input_mode_rejects_ctrl_c() {
        let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        let action = map_key(InputMode::Filter, key);
        assert_eq!(action, None);
    }

    #[test]
    fn normal_mode_maps_d_to_details() {
        let key = KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::ShowDetails));
    }

    #[test]
    fn normal_mode_maps_shift_l_to_related_logs() {
        let key = KeyEvent::new(KeyCode::Char('L'), KeyModifiers::SHIFT);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::LoadResourceLogs));
    }

    #[test]
    fn normal_mode_maps_m_to_manifest() {
        let key = KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::ShowManifest));
    }

    #[test]
    fn input_mode_maps_ctrl_m_and_ctrl_j_to_submit() {
        let ctrl_m = KeyEvent::new(KeyCode::Char('m'), KeyModifiers::CONTROL);
        let ctrl_j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::CONTROL);
        assert_eq!(
            map_key(InputMode::Command, ctrl_m),
            Some(Action::SubmitInput)
        );
        assert_eq!(
            map_key(InputMode::Command, ctrl_j),
            Some(Action::SubmitInput)
        );
    }

    #[test]
    fn normal_mode_maps_ctrl_m_and_ctrl_j_to_enter() {
        let ctrl_m = KeyEvent::new(KeyCode::Char('m'), KeyModifiers::CONTROL);
        let ctrl_j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::CONTROL);
        assert_eq!(
            map_key(InputMode::Normal, ctrl_m),
            Some(Action::EnterResource)
        );
        assert_eq!(
            map_key(InputMode::Normal, ctrl_j),
            Some(Action::EnterResource)
        );
    }

    #[test]
    fn normal_mode_maps_uppercase_confirmation_keys() {
        let yes = KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::SHIFT);
        let no = KeyEvent::new(KeyCode::Char('N'), KeyModifiers::SHIFT);
        assert_eq!(map_key(InputMode::Normal, yes), Some(Action::ConfirmYes));
        assert_eq!(map_key(InputMode::Normal, no), Some(Action::ConfirmNo));
    }

    #[test]
    fn normal_mode_maps_o_to_overview() {
        let key = KeyEvent::new(KeyCode::Char('o'), KeyModifiers::NONE);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::ToggleOverview));
    }

    #[test]
    fn normal_mode_maps_ctrl_digit_to_view_switch() {
        let key = KeyEvent::new(KeyCode::Char('3'), KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(3)));
    }

    #[test]
    fn normal_mode_maps_plain_digit_to_view_switch() {
        let key = KeyEvent::new(KeyCode::Char('6'), KeyModifiers::NONE);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(6)));
    }

    #[test]
    fn normal_mode_maps_ctrl_a_to_view_switch() {
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(1)));
    }

    #[test]
    fn normal_mode_maps_ctrl_backspace_to_view_switch() {
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(8)));
    }

    #[test]
    fn normal_mode_maps_ctrl_tab_to_view_switch() {
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(9)));
    }

    #[test]
    fn hotkey_signature_normalizes_modifiers() {
        let key = KeyEvent::new(
            KeyCode::Char('P'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        );
        assert_eq!(key_event_signature(key), Some("ctrl+shift+p".to_string()));
    }

    #[test]
    fn hotkey_spec_parses_common_tokens() {
        assert_eq!(
            normalize_hotkey_spec("shift+ctrl+F5"),
            Some("ctrl+shift+f5".to_string())
        );
        assert_eq!(
            normalize_hotkey_spec("ctrl+alt+1"),
            Some("ctrl+alt+1".to_string())
        );
        assert_eq!(
            normalize_hotkey_spec("ctrl+pgup"),
            Some("ctrl+pageup".to_string())
        );
    }

    #[test]
    fn normal_mode_maps_ctrl_space_to_view_zero() {
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(0)));
    }

    #[test]
    fn normal_mode_maps_ctrl_right_paren_to_view_zero() {
        let key = KeyEvent::new(KeyCode::Char(')'), KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(0)));
    }

    #[test]
    fn normal_mode_maps_plain_right_paren_to_view_zero() {
        let key = KeyEvent::new(KeyCode::Char(')'), KeyModifiers::NONE);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(0)));
    }

    #[test]
    fn normal_mode_maps_ctrl_esc_to_view_three() {
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::CONTROL);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::SwitchView(3)));
    }

    #[test]
    fn input_mode_maps_null_to_view_switch_zero() {
        let key = KeyEvent::new(KeyCode::Null, KeyModifiers::NONE);
        let action = map_key(InputMode::Command, key);
        assert_eq!(action, Some(Action::SwitchView(0)));
    }

    #[test]
    fn normal_mode_maps_shift_semicolon_to_command() {
        let key = KeyEvent::new(KeyCode::Char(';'), KeyModifiers::SHIFT);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::StartCommand));
    }

    #[test]
    fn normal_mode_maps_shift_period_to_jump() {
        let key = KeyEvent::new(KeyCode::Char('.'), KeyModifiers::SHIFT);
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::StartJump));
    }

    #[test]
    fn input_mode_maps_ctrl_shift_digit_symbols_to_view_switch() {
        let ctrl_shift_1 = KeyEvent::new(
            KeyCode::Char('!'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        );
        let ctrl_shift_5 = KeyEvent::new(
            KeyCode::Char('%'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        );
        let ctrl_shift_9 = KeyEvent::new(
            KeyCode::Char('('),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        );
        assert_eq!(
            map_key(InputMode::Command, ctrl_shift_1),
            Some(Action::SwitchView(1))
        );
        assert_eq!(
            map_key(InputMode::Command, ctrl_shift_5),
            Some(Action::SwitchView(5))
        );
        assert_eq!(
            map_key(InputMode::Command, ctrl_shift_9),
            Some(Action::SwitchView(9))
        );
    }

    #[test]
    fn normal_mode_maps_ctrl_alt_digit_to_delete_view() {
        let key = KeyEvent::new(
            KeyCode::Char('4'),
            KeyModifiers::CONTROL | KeyModifiers::ALT,
        );
        let action = map_key(InputMode::Normal, key);
        assert_eq!(action, Some(Action::DeleteView(4)));
    }

    #[test]
    fn input_mode_maps_ctrl_alt_digit_to_delete_view() {
        let key = KeyEvent::new(
            KeyCode::Char('7'),
            KeyModifiers::CONTROL | KeyModifiers::ALT,
        );
        let action = map_key(InputMode::Command, key);
        assert_eq!(action, Some(Action::DeleteView(7)));
    }
}
