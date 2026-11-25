export function PlaceholderPage({ title }: { title: string }) {
  return (
    <section className="rounded-lg border border-dashed p-8 text-sm text-muted-foreground">
      {title} is being rebuilt in the new UI.
    </section>
  )
}
