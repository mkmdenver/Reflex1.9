"""Reflex app to connect to OpenAI Codex/GPT API."""

import os
from dotenv import load_dotenv
from openai import OpenAI
import reflex as rx

# Load environment variables
load_dotenv()


class State(rx.State):
    """The app state."""

    prompt: str = ""
    response: str = ""
    is_loading: bool = False
    error_message: str = ""

    def set_prompt(self, value: str):
        """Set the prompt value."""
        self.prompt = value

    async def generate_code(self):
        """Generate code using OpenAI API."""
        if not self.prompt:
            self.error_message = "Please enter a prompt"
            return

        self.is_loading = True
        self.error_message = ""
        self.response = ""
        yield

        try:
            # Get API key from environment
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                self.error_message = "OpenAI API key not found. Please set OPENAI_API_KEY in .env file"
                self.is_loading = False
                yield
                return

            # Initialize OpenAI client
            client = OpenAI(api_key=api_key)

            # Call OpenAI API (using GPT-4 as Codex is deprecated)
            completion = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful coding assistant. Generate clean, well-commented code."},
                    {"role": "user", "content": self.prompt}
                ],
                max_tokens=1000,
                temperature=0.7
            )

            # Extract response
            self.response = completion.choices[0].message.content

        except Exception as e:
            self.error_message = f"Error: {str(e)}"

        finally:
            self.is_loading = False
            yield


def index() -> rx.Component:
    """The main page."""
    return rx.container(
        rx.vstack(
            rx.heading("Reflex Codex Connection", size="9", margin_bottom="1em"),
            rx.text(
                "Connect to OpenAI's GPT API for code generation",
                size="4",
                color="gray",
                margin_bottom="2em",
            ),
            rx.vstack(
                rx.text("Enter your coding prompt:", font_weight="bold"),
                rx.text_area(
                    value=State.prompt,
                    on_change=State.set_prompt,
                    placeholder="e.g., Write a Python function to calculate fibonacci numbers",
                    width="100%",
                    min_height="100px",
                ),
                rx.button(
                    "Generate Code",
                    on_click=State.generate_code,
                    loading=State.is_loading,
                    width="100%",
                    margin_top="1em",
                ),
                width="100%",
                spacing="2",
            ),
            rx.cond(
                State.error_message != "",
                rx.callout(
                    State.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                    role="alert",
                    margin_top="1em",
                ),
            ),
            rx.cond(
                State.response != "",
                rx.vstack(
                    rx.text("Generated Code:", font_weight="bold", margin_top="2em"),
                    rx.code_block(
                        State.response,
                        language="python",
                        width="100%",
                    ),
                    width="100%",
                    spacing="2",
                ),
            ),
            spacing="4",
            width="100%",
            max_width="800px",
        ),
        padding="2em",
    )


app = rx.App()
app.add_page(index, title="Reflex Codex")
