package com.github.avlomakin

import org.junit.jupiter.api.Test

class ImmutableSuffixArrayTest {
    @Test
    fun `returns elements that contains substring`() {
        val suffix = getSuffixArrayFor(
            Element(1, setOf("ASDK", "EEF")),
            Element(2, setOf("DKI")),
            Element(3, setOf("QQQ")),
        )

        val result = suffix.elementContainsIds("DK")

        assert(result == setOf(1L, 2L))
    }

    @Test
    fun `array handles strings with Non-ASCII characters`() {
        val suffix = getSuffixArrayFor(
            Element(1, setOf("BSDK", "EEF")),
            Element(2, setOf("Сам Рашн Текст")),
            Element(3, setOf("QQQ")),
        )

        val result = suffix.elementContainsIds("Рашн")

        assert(result == setOf(2L))
    }

    @Test
    fun `returns empty sequence when searched substring is outside of bounds`() {
        val suffix = getSuffixArrayFor(
            Element(1, setOf("BSDK", "EEF")),
            Element(2, setOf("DKI")),
            Element(3, setOf("KKK")),
        )

        val resultOutsideRight = suffix.elementContainsIds("QQ")
        val resultOutsideLeft = suffix.elementContainsIds("AA")
        assert(resultOutsideRight.isEmpty())
        assert(resultOutsideLeft.isEmpty())
    }

    private fun getSuffixArrayFor(vararg element: Element): ImmutableSuffixArray<Element> {
        return ImmutableSuffixArray.generateSuffixArray(
            listOf(
                *element
            )
        ) { it.aliases }
    }

    private fun ImmutableSuffixArray<Element>.elementContainsIds(str: String): Set<Long> {
        return this.elementContainsSequence(str).map { it.id }.toSet()
    }
}