import typer 
import random 
import time

from src.services.whatsapp.twilio_sender import run_send_whatsapp
from src.services.whatsapp.utils.config import LOGGER


app = typer.Typer(help="Formats and sends a WhatsApp report summary from a JSON Filings file.")


@app.command()
def main_wa_workflow(
    filings_data_path: str = typer.Option(
        ...,
        help="Path to the JSON file containing filings data."
    ),
    to_number: str = typer.Option(
        ...,
        help="Recipient's WhatsApp number in international format, e.g., +1234567890"
    )
): 
    try:
        recipient_list = [num_recipient.strip() for num_recipient in to_number.split(",")]
        LOGGER.info(f"Starting WhatsApp workflow for {len(recipient_list)} recipients.")

        for num_recipient in recipient_list:
            summary = run_send_whatsapp(filings_data_path, num_recipient)

            typer.echo(f"WhatsApp Summary for number ...{num_recipient[-4:]}: "
                       "{summary.get('sent')} sent, {summary.get('failed')} failed")

            if summary.get("failed") > 0:
                typer.echo("\nErrors:")
                for error in summary.get("errors"):
                    typer.echo(f" - {error}")
            else:
                typer.echo("All messages sent successfully.")

            time.sleep(random.uniform(1, 5))

    except Exception as error:
        LOGGER.critical(f"CLI failed with unexpected error: {error}", exc_info=True)
        typer.echo("Critical failure in workflow. Check logs for details.")


if __name__ == '__main__':
    app()
