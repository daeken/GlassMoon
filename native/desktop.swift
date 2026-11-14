import AppKit
import ApplicationServices
import CoreGraphics
import ScreenCaptureKit

struct DesktopError: Error {
    let code: String
    let message: String
    init(_ code: String, _ message: String) {
        self.code = code
        self.message = message
    }
}

func integer(_ args: [String: Any], _ key: String) throws -> Int {
    guard let value = args[key] as? NSNumber, CFGetTypeID(value) != CFBooleanGetTypeID(),
          let result = Int(value.stringValue) else {
        throw DesktopError("invalid_arguments", "\(key) must be an integer")
    }
    return result
}

func post(_ event: CGEvent?) throws {
    guard let event else {
        throw DesktopError("input_failed", "Could not create a macOS input event")
    }
    event.post(tap: .cghidEventTap)
}

func key(_ code: CGKeyCode) throws {
    for down in [true, false] {
        try post(CGEvent(keyboardEventSource: nil, virtualKey: code, keyDown: down))
    }
}

func typeText(_ text: String, pid: Int?) throws {
    let system = pid.map { AXUIElementCreateApplication(pid_t($0)) } ?? AXUIElementCreateSystemWide()
    var focused: CFTypeRef?
    guard AXUIElementCopyAttributeValue(system, kAXFocusedUIElementAttribute as CFString, &focused) == .success,
          let focused, CFGetTypeID(focused) == AXUIElementGetTypeID() else {
        throw DesktopError("no_text_control", "No accessible focused text control")
    }
    let control = unsafeDowncast(focused, to: AXUIElement.self)
    guard AXUIElementSetAttributeValue(control, kAXSelectedTextAttribute as CFString, text as CFString) == .success else {
        throw DesktopError("text_control_unsupported", "The focused control does not allow text insertion through Accessibility")
    }
}

func axAttribute(_ element: AXUIElement, _ attribute: String) -> CFTypeRef? {
    var value: CFTypeRef?
    guard AXUIElementCopyAttributeValue(element, attribute as CFString, &value) == .success else {
        return nil
    }
    return value
}

func accessibilitySnapshot(_ pid: Int) throws -> [String: Any] {
    guard AXIsProcessTrusted() else {
        throw DesktopError("accessibility_required", "Allow Accessibility for the GlassMoon desktop launcher in macOS Settings")
    }
    _ = AXUIElementSetMessagingTimeout(AXUIElementCreateSystemWide(), 0.15)
    let app = AXUIElementCreateApplication(pid_t(pid))
    guard axAttribute(app, kAXRoleAttribute) != nil else {
        throw DesktopError("target_unavailable", "Could not read the configured app's accessibility tree")
    }
    let limit = 55
    let deadline = ProcessInfo.processInfo.systemUptime + 2
    var pending = [(app, 0)]
    var rows = [[String: Any]]()
    var truncated = false
    while let (element, depth) = pending.popLast() {
        if rows.count == limit || ProcessInfo.processInfo.systemUptime >= deadline {
            truncated = true
            break
        }
        var owner: pid_t = 0
        guard AXUIElementGetPid(element, &owner) == .success, owner == pid_t(pid) else {
            continue
        }
        let role = axAttribute(element, kAXRoleAttribute) as? String ?? "AXUnknown"
        var row: [String: Any] = ["depth": depth, "role": String(role.prefix(80))]
        if let label = (axAttribute(element, kAXTitleAttribute) as? String)
            .flatMap({ $0.isEmpty ? nil : $0 }) ?? axAttribute(element, kAXDescriptionAttribute) as? String,
           !label.isEmpty {
            row["label"] = String(label.prefix(80))
        }
        rows.append(row)
        let attribute = (depth == 0 ? kAXWindowsAttribute : kAXChildrenAttribute) as CFString
        var count = 0
        if AXUIElementGetAttributeValueCount(element, attribute, &count) == .success, count > 0 {
            let capacity = min(20, limit - rows.count - pending.count)
            if depth >= 7 || capacity <= 0 {
                truncated = true
                continue
            }
            var elements: CFArray?
            if AXUIElementCopyAttributeValues(element, attribute, 0, min(count, capacity), &elements) == .success,
               let children = elements as? [AXUIElement] {
                pending.append(contentsOf: children.reversed().map { ($0, depth + 1) })
                truncated = truncated || children.count < count
            }
        }
    }
    return ["target_pid": pid, "nodes": rows, "truncated": truncated]
}

func requireTarget(_ pid: Int, at point: CGPoint) throws {
    let windows = CGWindowListCopyWindowInfo([.optionOnScreenOnly, .excludeDesktopElements], kCGNullWindowID)
        as? [[String: Any]] ?? []
    let target = windows.first { window in
        guard window[kCGWindowLayer as String] as? Int == 0,
              let bounds = window[kCGWindowBounds as String] as? [String: Any],
              let rectangle = CGRect(dictionaryRepresentation: bounds as CFDictionary) else {
            return false
        }
        return rectangle.contains(point)
    }
    guard target?[kCGWindowOwnerPID as String] as? Int == pid else {
        throw DesktopError("target_outside_window", "The requested location is outside the configured macOS app")
    }
}

func capture(_ filter: SCContentFilter, width: Int, height: Int) async throws -> [String: Any] {
    let config = SCStreamConfiguration()
    config.width = width
    config.height = height
    config.showsCursor = false
    let image = try await SCScreenshotManager.captureImage(contentFilter: filter, configuration: config)
    let bitmap = NSBitmapImageRep(cgImage: image)
    for quality in [0.60, 0.36, 0.20, 0.10, 0.05] {
        if let jpeg = bitmap.representation(using: .jpeg, properties: [.compressionFactor: quality]),
           jpeg.count <= 21000 {
            return [
                "width": width, "height": height,
                "image": ["type": "image", "url": "data:image/jpeg;base64," + jpeg.base64EncodedString()]
            ]
        }
    }
    throw DesktopError("image_too_large", "Screenshot exceeds the IPC image budget")
}

@main
struct Desktop {
    static func main() async {
        let reply: [String: Any]
        do {
            let data = FileHandle.standardInput.readDataToEndOfFile()
            guard let args = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let action = args["action"] as? String else {
                throw DesktopError("invalid_arguments", "Expected an action object")
            }
            reply = try await perform(action, args)
        } catch let error as DesktopError {
            reply = ["error": error.code, "message": error.message]
        } catch {
            reply = ["error": "desktop_error", "message": "macOS desktop operation failed"]
        }
        if let data = try? JSONSerialization.data(withJSONObject: reply) {
            FileHandle.standardOutput.write(data)
        }
    }

    static func perform(_ action: String, _ args: [String: Any]) async throws -> [String: Any] {
        let displayID = CGMainDisplayID()
        let bounds = CGDisplayBounds(displayID)
        let width = max(1, min(640, Int(bounds.width)))
        let height = max(1, Int(bounds.height * Double(width) / bounds.width))
        switch action {
        case "status":
            return [
                "screen_recording": CGPreflightScreenCaptureAccess(),
                "accessibility": CGPreflightPostEventAccess(),
                "width": width, "height": height, "display": Int(displayID)
            ]
        case "accessibility_snapshot":
            let pid = try integer(args, "target_pid")
            guard pid > 0, pid <= Int(Int32.max) else {
                throw DesktopError("invalid_target", "Configure a macOS process ID for an accessibility snapshot")
            }
            return try accessibilitySnapshot(pid)
        case "screenshot":
            guard CGPreflightScreenCaptureAccess() else {
                throw DesktopError("screen_recording_required", "Allow Screen Recording for the GlassMoon desktop launcher in macOS Settings")
            }
            let content = try await SCShareableContent.excludingDesktopWindows(false, onScreenWindowsOnly: true)
            guard let display = content.displays.first(where: { $0.displayID == displayID }) else {
                throw DesktopError("display_unavailable", "Cannot find the main display")
            }
            let filter = SCContentFilter(display: display, excludingWindows: [])
            var result = try await capture(filter, width: width, height: height)
            result["display"] = Int(displayID)
            return result
        case "windows", "window_screenshot":
            let pid = try integer(args, "target_pid")
            guard pid > 0, pid <= Int(Int32.max) else {
                throw DesktopError("invalid_target", "Configure a macOS process ID for window capture")
            }
            guard CGPreflightScreenCaptureAccess() else {
                throw DesktopError("screen_recording_required", "Allow Screen Recording for the GlassMoon desktop launcher in macOS Settings")
            }
            let content = try await SCShareableContent.excludingDesktopWindows(true, onScreenWindowsOnly: true)
            let windows = content.windows.filter { window in
                window.owningApplication?.processID == pid_t(pid) && window.windowLayer == 0
                    && window.frame.width > 1 && window.frame.height > 1
            }
            if action == "windows" {
                return ["windows": windows.prefix(30).map { window in
                    ["id": Int(window.windowID), "title": String((window.title ?? "").prefix(200)),
                     "width": Int(window.frame.width), "height": Int(window.frame.height)] as [String: Any]
                }]
            }
            let id = try integer(args, "window_id")
            guard let window = windows.first(where: { Int($0.windowID) == id }) else {
                throw DesktopError("window_unavailable", "The selected window is unavailable for this app")
            }
            let scale = min(1, 640 / max(window.frame.width, window.frame.height))
            let filter = SCContentFilter(desktopIndependentWindow: window)
            var result = try await capture(filter, width: max(1, Int(window.frame.width * scale)),
                                           height: max(1, Int(window.frame.height * scale)))
            result["window_id"] = Int(window.windowID)
            return result
        case "click", "type_text", "press", "scroll":
            guard CGPreflightPostEventAccess() else {
                throw DesktopError("accessibility_required", "Allow Accessibility for the GlassMoon desktop launcher in macOS Settings")
            }
            if let pid = args["target_pid"] as? Int,
               NSWorkspace.shared.frontmostApplication.map({ Int($0.processIdentifier) }) != pid {
                throw DesktopError("target_inactive", "The configured macOS app is not frontmost")
            }
        default:
            throw DesktopError("unknown_action", "Unknown desktop action")
        }

        if action == "click" {
            let x = try integer(args, "x")
            let y = try integer(args, "y")
            guard (0..<width).contains(x), (0..<height).contains(y) else {
                throw DesktopError("invalid_position", "Use coordinates inside the last screenshot")
            }
            let point = CGPoint(
                x: bounds.minX + Double(x) * bounds.width / Double(width),
                y: bounds.minY + Double(y) * bounds.height / Double(height)
            )
            if let pid = args["target_pid"] as? Int {
                try requireTarget(pid, at: point)
            }
            for event in [CGEventType.leftMouseDown, .leftMouseUp] {
                try post(CGEvent(mouseEventSource: nil, mouseType: event, mouseCursorPosition: point, mouseButton: .left))
            }
        } else if action == "type_text" {
            guard let text = args["text"] as? String, text.utf16.count <= 256 else {
                throw DesktopError("invalid_text", "Use up to 256 UTF-16 units per call")
            }
            try typeText(text, pid: args["target_pid"] as? Int)
        } else if action == "press" {
            let supported: [String: CGKeyCode] = [
                "Enter": 36, "Tab": 48, "Space": 49, "Backspace": 51,
                "Escape": 53, "ArrowLeft": 123, "ArrowRight": 124,
                "ArrowDown": 125, "ArrowUp": 126
            ]
            guard let name = args["key"] as? String, let code = supported[name] else {
                throw DesktopError("invalid_key", "Use Enter, Tab, Space, Backspace, Escape, or an arrow key")
            }
            try key(code)
        } else {
            let dx = try integer(args, "dx")
            let dy = try integer(args, "dy")
            guard (-3000...3000).contains(dx), (-3000...3000).contains(dy) else {
                throw DesktopError("invalid_scroll", "Scroll deltas must be within 3000 pixels")
            }
            if let pid = args["target_pid"] as? Int {
                guard let point = CGEvent(source: nil)?.location else {
                    throw DesktopError("input_failed", "Could not read the pointer location")
                }
                try requireTarget(pid, at: point)
            }
            try post(CGEvent(scrollWheelEvent2Source: nil, units: .pixel, wheelCount: 2,
                             wheel1: Int32(-dy), wheel2: Int32(-dx), wheel3: 0))
        }
        return ["action": action, "delivered": true]
    }
}
