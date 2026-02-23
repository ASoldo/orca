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
        KeyCode::Char('j') | KeyCode::Down => Some(Action::Down),
        KeyCode::Char('k') | KeyCode::Up => Some(Action::Up),
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
        KeyCode::Char('p') => Some(Action::StartPortForwardPrompt),
        KeyCode::Char('o') => Some(Action::ToggleOverview),
        KeyCode::Char('d') if key.modifiers.is_empty() => Some(Action::ShowDetails),
        KeyCode::Char('y') => Some(Action::ConfirmYes),
        KeyCode::Char('n') => Some(Action::ConfirmNo),
        KeyCode::Tab if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::SwitchView(9))
        }
        KeyCode::Tab => Some(Action::ToggleFocus),
        KeyCode::Enter => Some(Action::EnterResource),
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
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::ALT) && c.is_ascii_digit() => {
            Some(Action::SwitchView(c.to_digit(10).unwrap_or(0) as u8))
        }
        _ => None,
    }
}

fn map_ctrl_number(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c.to_digit(10).unwrap_or(0) as u8),
        '@' => Some(2),
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
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            map_ctrl_number(c).map(Action::SwitchView)
        }
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::ALT) && c.is_ascii_digit() => {
            Some(Action::SwitchView(c.to_digit(10).unwrap_or(0) as u8))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, map_key};
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
    fn input_mode_maps_alt_digit_to_view_switch() {
        let key = KeyEvent::new(KeyCode::Char('4'), KeyModifiers::ALT);
        let action = map_key(InputMode::Command, key);
        assert_eq!(action, Some(Action::SwitchView(4)));
    }
}
