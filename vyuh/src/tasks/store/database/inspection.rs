//! Task inspection without loading private ordered join membership.

use super::*;

impl DbTaskStore {
    /// Returns one deterministic page using the existing count/read query pair.
    pub(in super::super) async fn list_tasks_impl(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskRuntimeError> {
        let table = Self::table();
        let mut pool = self.pool.clone();
        let page = apply_filter(db::from(&table), &table, &filter)
            .sort(table.created_at.desc())
            .sort(table.id.desc())
            .page::<TaskRow, _>(
                db::Pagination {
                    page_num: filter.page,
                    page_size: filter.per_page,
                },
                &mut pool,
            )
            .await?;
        Ok(crate::routes::Page::new(
            Self::into_records(page.items)?,
            page.total,
            page.page,
            page.per_page,
        ))
    }
}
