"""Wallet management for Solana transactions."""

import base58
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from spl.token.instructions import get_associated_token_address

from core.pubkeys import SystemAddresses


class Wallet:
    """Manages a Solana wallet for trading operations."""

    def __init__(self, private_key: str):
        """Initialize wallet from private key.

        Args:
            private_key: Base58 encoded private key
        """
        self._private_key = private_key
        self._keypair = self._load_keypair(private_key)

    @property
    def pubkey(self) -> Pubkey:
        """Get the public key of the wallet."""
        return self._keypair.pubkey()

    @property
    def keypair(self) -> Keypair:
        """Get the keypair for signing transactions."""
        return self._keypair

    def get_associated_token_address(
        self, mint: Pubkey, token_program_id: Pubkey | None = None
    ) -> Pubkey:
        """Get the associated token account address for a mint.

        Args:
            mint: Token mint address
            token_program_id: Token program (TOKEN or TOKEN_2022). Defaults to TOKEN_2022_PROGRAM
        """
        if token_program_id is None:
            token_program_id = SystemAddresses.TOKEN_2022_PROGRAM
        return get_associated_token_address(self.pubkey, mint, token_program_id)

    @staticmethod
    def _load_keypair(private_key: str) -> Keypair:
        """Load keypair from private key.

        Args:
            private_key: Base58 encoded private key

        Returns:
            Solana keypair
        """
        # Pasted secrets often carry stray whitespace or quotes; the error
        # messages never echo the key itself.
        key = private_key.strip().strip("'\"").strip()
        if len(key.split()) > 1:
            raise ValueError(  # noqa: TRY003
                "SOLANA_PRIVATE_KEY contains spaces - this looks like a seed phrase. "
                "Use the base58 private key (one long string, no spaces) from the "
                "wallet's 'Export private key' option."
            )
        try:
            private_key_bytes = base58.b58decode(key)
            return Keypair.from_bytes(private_key_bytes)
        except ValueError:
            raise ValueError(  # noqa: TRY003
                f"SOLANA_PRIVATE_KEY is not a valid base58 private key "
                f"({len(key)} characters; expected about 87-88)"
            ) from None
